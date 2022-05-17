package scala.sql.table.socket

import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.io.Source
import scala.log.Logging

class SocketSourceFunction[T](
  host: String,
  port: Int,
  deserializer: DeserializationSchema[T]
) extends RichSourceFunction[T] with Logging{
  var stop = false
  private var socket: Socket = _

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    logInfo(s"Connecting to $host:$port")
    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException =>
        logError("Connecting error", e)
        return
    }
    logInfo(s"Connected to $host:$port")

    try {
      val inputStream = socket.getInputStream()
      val lines = Source.fromInputStream(inputStream, "utf-8").getLines()
      while (!stop && lines.hasNext) {
        val line = lines.next().trim
        if(line.nonEmpty){
          val data = deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8))
          ctx.collect(data)
        }
      }
    } catch {
      case e:Exception =>
        e.printStackTrace()
    }
  }

  override def cancel(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
      logInfo(s"Closed socket to $host:$port")
    }

    this.stop = true
  }
}
