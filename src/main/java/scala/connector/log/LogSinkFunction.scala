package scala.connector.log

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.{RuntimeContextInitializationContextAdapters, SerializationSchema}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.data.RowData
import org.apache.flink.types.{Row, RowKind}

import scala.log.Logging
import scala.reflect.ClassTag

class LogSinkFunction[T: ClassTag](
  logLevel: String,
  serializer: SerializationSchema[T]
) extends RichSinkFunction[T] with Logging {
  var out: String => Unit = _
  var getOp: Any => String = _

  override def open(parameters: Configuration): Unit = {
    out = logLevel.toLowerCase match {
      case "error" => x => logError(x)
      case "warn" => x => logWarning(x)
      case "info" => x => logInfo(x)
      case "debug" => x => logDebug(x)
      case "print" => x => println(x)
    }
    getOp = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]] match {
      case x if x == classOf[RowData] => x => x.asInstanceOf[RowData].getRowKind.toString
      case x if x == classOf[Row] => x => x.asInstanceOf[Row].getKind.toString
      case _ => x => ""
    }
    serializer.open(RuntimeContextInitializationContextAdapters.serializationAdapter(getRuntimeContext()))
  }

  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    val op = getOp(value)
    val body = serializer.serialize(value)
    val line = new String(body, StandardCharsets.UTF_8)
    out(op + ": "+ line)
  }


}
