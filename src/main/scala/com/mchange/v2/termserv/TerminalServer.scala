package com.mchange.v2.termserv;

import java.net.InetSocketAddress

import java.io.ByteArrayOutputStream

import java.nio._
import java.nio.channels._

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger

import scala.collection._

import com.mchange.sc.v1.log.MLevel._;

object TerminalServer {
  implicit val Logger = mlogger( this )

  val BufferSize = 8 * 1024

  val DumpStack : PartialFunction[Throwable, Unit] = { case e : Exception => e.printStackTrace }

  def main( argv : Array[String] ) : Unit = new TerminalServer( argv(0).toInt )
}
class TerminalServer( val port : Int, val numClientThreads : Int = 3 ) {
  import TerminalServer.{DumpStack,Logger}

  private val counter    = new AtomicInteger(0)
  private val threadPool = Executors.newFixedThreadPool( numClientThreads )

  private val selectorThread = new SelectorThread

  // MT: Protected by its own lock
  private val openChannels : mutable.Set[Channel] = mutable.Set.empty[Channel]

  private def markOpen( channel : Channel ) : Unit   = openChannels.synchronized { 
    openChannels += channel; 
    TRACE.log( s"Marked as open channel ${channel} (now ${openChannels.size} open channels)" ) 
  }
  private def markClosed( channel : Channel ) : Unit = openChannels.synchronized { 
    openChannels -= channel 
    TRACE.log( s"Marked as closed channel ${channel} (now ${openChannels.size} open channels)" ) 
  }
  private def closeAll : Unit = openChannels.synchronized { 
    openChannels.foreach{ c => try c.close catch DumpStack } 
    openChannels.clear
  }

  // MT: Protected by its own lock
  private val toCancelKeys = mutable.Set.empty[SelectionKey]
  private def markToCancel( key : SelectionKey ) : Unit = toCancelKeys.synchronized {
    toCancelKeys += key;
  }
  private def processToCancel : Unit = { // only from SelectorThread!
    toCancelKeys.synchronized {
      toCancelKeys.foreach( _.cancel )
      toCancelKeys.clear
    }
  }

  // MT: Protected by its own lock
  private val toReadKeys = mutable.Set.empty[SelectionKey]
  private def markToRead( key : SelectionKey ) : Unit = toReadKeys.synchronized {
    toReadKeys += key
  }
  private def processToRead : Unit = { // only from SelectorThread
    toReadKeys.synchronized {
      toReadKeys.foreach( _.interestOps( SelectionKey.OP_READ ) )
      toReadKeys.clear
    }
  }

  def close : Unit = {
    selectorThread.shouldStop = true
  }
  def closed : Boolean = !selectorThread.isAlive

  private class SelectorThread extends Thread( "TerminalServer.SelectorThread" ) {

    @volatile var shouldStop : Boolean = false;

    val selector = Selector.open;
    TRACE.log( s"Opened Selector: ${selector}" )

    override def run : Unit = {
      try {
        TRACE.log("SelectorThread started.")

        val ssc = ServerSocketChannel.open
        ssc.configureBlocking( false )
        markOpen( ssc )
        TRACE.log( s"Opened ServerSocketChannel: ${ssc}" )

        val addr = new InetSocketAddress( port )
        ssc.socket().bind( addr );
        TRACE.log( s"Bound to port: ${port}" )

        ssc.register( selector, SelectionKey.OP_ACCEPT );

        while ( !shouldStop ) {
          processToCancel
          processToRead

          TRACE.log( "Entering blocking select." )
          val updatedKeys = selector.select();
          TRACE.log( s"Select returned ${updatedKeys} keys." )

          if ( updatedKeys > 0 ) {
            import scala.collection.JavaConversions._

            val keys : mutable.Set[SelectionKey] = selector.selectedKeys

            while (! keys.isEmpty ) {
              val key = keys.head
              keys.remove( key )

              try {
                if ( key.isValid ) {
                  TRACE.log( s"Processing key ${key}." )
                  handleKey( key )
                } else {
                  TRACE.log( s"Ignoring invalid key ${key}." )
                }
              } catch {
                case cke : CancelledKeyException => {
                  val sc = key.channel
                  TRACE.log( s"Key canceled, presumably connection closed closed. ${sc}" )
                  closeDetected( sc, key )
                }
              }
            }
          }
        } 
      } finally {
        TRACE.log( s"Closing all open channels." )
        closeAll
        TRACE.log( s"Closed all open channels." )
      }

      TRACE.log("SelectorThread completed. Exiting.")
    }

    private def handleKey( key : SelectionKey ) : Unit = {
      if ( key.isAcceptable() ) handleAccept( key );
      else if ( key.isReadable() ) handleRead( key );
      else WARNING.log( s"Ignoring unexpected SelectionKey: ${key}" )
    }

    private def handleAccept( key : SelectionKey ) : Unit = {
      val sc = key.channel.asInstanceOf[ServerSocketChannel].accept
      sc.configureBlocking( false )
      markOpen( sc )

      val rec = new ClientRec
      TRACE.log( s"Accepted connection #${rec.index}: ${sc}" )
      sc.register( selector, SelectionKey.OP_READ, rec )
    }

    private def handleRead( key : SelectionKey ) : Unit = {
      val sc = key.channel; // already in openChannels
      try {
        sc.configureBlocking( false )
        TRACE.log( s"Ready for read: ${sc}" )

        val rec = key.attachment.asInstanceOf[ClientRec]

        key.interestOps( 0 )

        asyncRead( sc.asInstanceOf[SocketChannel], rec, key );
      } catch closeDetectedOnException( sc, key )
    }

    private def closeDetected( sc : SelectableChannel, key : SelectionKey ) : Unit = {
      try {
        markClosed( sc )
        sc.close
        markToCancel( key )
        TRACE.log( s"Closed: ${sc}" )
      } catch DumpStack
    }

    private def closeDetectedOnException( sc : SelectableChannel, key : SelectionKey ) : PartialFunction[Throwable,Unit] = {
      case e : ClosedChannelException => {
        TRACE.log( s"Close detected on read. ${sc}" )
        closeDetected( sc, key )
      }
    }

    private def asyncRead( sc : SocketChannel, rec : ClientRec, key : SelectionKey ) : Unit = {

      val handler = new Runnable {
        override def run : Unit = {
          try {
            val buf = rec.readBuffer

            val baos = new ByteArrayOutputStream( TerminalServer.BufferSize );
            val out  = Channels.newChannel( baos )

            buf.clear();

            var bytesRead = sc.read(buf);
            while( bytesRead > 0 || buf.position() > 0 ) { // in case of partial writes
              TRACE.log( s"bytesRead: ${bytesRead}, position: ${buf.position()}" )
              buf.flip;
              out.write( buf );
              buf.compact
              bytesRead = sc.read(buf)
            }
            TRACE.log( s"Finished data write. [bytesRead: ${bytesRead}, position: ${buf.position()}]" )

            print( new String( baos.toByteArray ) )

            // this stuff might not be cool, manipulating the SelectionKey from a second Thread :(
            if ( bytesRead < 0 ) { // disconnected
              closeDetected( sc, key )
            } else {
              markToRead( key )
              selector.wakeup
            }
          } catch closeDetectedOnException( sc, key )
        }
      }
      threadPool.execute( handler )
    }

    private def createReadBuffer : ByteBuffer = ByteBuffer.allocate( TerminalServer.BufferSize )

    private class ClientRec {
      val index : Int  = counter.getAndIncrement
      val readBuffer : ByteBuffer = createReadBuffer
    }
  }

  selectorThread.start()
  TRACE.log( "SelectorThread started. TerminalServer constructed." )
}
