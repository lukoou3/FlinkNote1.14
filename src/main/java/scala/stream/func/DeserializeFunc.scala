package scala.stream.func

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, RuntimeContextInitializationContextAdapters}
import org.apache.flink.configuration.Configuration

class DeserializeFunc[OUT](
  deserializer: DeserializationSchema[OUT]
) extends RichMapFunction[Array[Byte], OUT]{

  override def open(parameters: Configuration): Unit = {
    deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()))
  }

  override def map(value: Array[Byte]): OUT = {
    deserializer.deserialize(value)
  }
}
