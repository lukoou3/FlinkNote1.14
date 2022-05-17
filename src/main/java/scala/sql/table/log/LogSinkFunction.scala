package scala.sql.table.log

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.data.RowData
import org.apache.flink.types.Row

import scala.log.Logging

class LogSinkFunction[T](
  logLevel: String,
  serializer: SerializationSchema[T]
) extends RichSinkFunction[T] with Logging {
  var out: String => Unit = _

  override def open(parameters: Configuration): Unit = {
    out = logLevel.toLowerCase match {
      case "error" => x => logError(x)
      case "warn" => x => logWarning(x)
      case "info" => x => logInfo(x)
      case "debug" => x => logDebug(x)
    }
  }

  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    val op = value match {
      case row:RowData => row.getRowKind
      case row:Row => row.getKind
      case _ => ""
    }
    val body = serializer.serialize(value)
    val line = new String(body, StandardCharsets.UTF_8)
    out(op + ": "+ line)
  }


}
