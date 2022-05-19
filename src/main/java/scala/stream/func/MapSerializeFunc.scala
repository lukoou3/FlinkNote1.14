package scala.stream.func

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.{RuntimeContextInitializationContextAdapters, SerializationSchema}
import org.apache.flink.configuration.Configuration

class MapSerializeFunc[IN](
  serializer: SerializationSchema[IN]
) extends RichMapFunction[IN, Array[Byte]]{
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    serializer.open(RuntimeContextInitializationContextAdapters.serializationAdapter(getRuntimeContext()))
  }

  override def map(value: IN): Array[Byte] = {
    serializer.serialize(value)
  }
}
