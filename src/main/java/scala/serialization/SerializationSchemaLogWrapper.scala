package scala.serialization

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.types.Row

import scala.log.Logging
import scala.reflect.ClassTag


class SerializationSchemaLogWrapper[T:ClassTag](
  serializer: SerializationSchema[T],
  logLevel: String = "warn"
) extends SerializationSchema[T] with Logging{
  val getRowKind: Any => String = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]] match {
    case x if x == classOf[RowData] => x => x.asInstanceOf[RowData].getRowKind.toString
    case x if x == classOf[Row] => x => x.asInstanceOf[Row].getKind.toString
    case _ => null
  }

  override def open(context: SerializationSchema.InitializationContext): Unit = {
    serializer.open(context)
  }

  override def serialize(element: T): Array[Byte] = {
    val body = serializer.serialize(element)
    logElement(element, body)
    body
  }

  def logElement(value: T, body: Array[Byte]): Unit = {
    val line = if(getRowKind == null){
      new String(body, StandardCharsets.UTF_8)
    }else{
      getRowKind(value) + ": " + new String(body, StandardCharsets.UTF_8)
    }

    if(logLevel == "error"){
      logError(line)
    }else if(logLevel == "warn"){
      logWarning(line)
    }else{
      logInfo(line)
    }
  }
}
