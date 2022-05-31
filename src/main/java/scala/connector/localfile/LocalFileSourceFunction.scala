package scala.connector.localfile

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.{DeserializationSchema, RuntimeContextInitializationContextAdapters}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.log.Logging

class LocalFileSourceFunction[T](
  filePath: String,
  sleep: Long = 10,
  numberOfRowsForSubtask: Long = Long.MaxValue,
  deserializer: DeserializationSchema[T]
) extends RichParallelSourceFunction[T] with Logging {
  var stop = false
  val lineBytes = ArrayBuffer[Array[Byte]]()

  override def open(parameters: Configuration): Unit = {
    deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()))
    Source.fromFile(filePath, "utf-8").getLines().filter(_.trim != "").foreach{ line =>
      lineBytes += line.getBytes(StandardCharsets.UTF_8)
    }
  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    var i = 0
    val step = 1
    var rows = 0

    while (!stop && rows < numberOfRowsForSubtask) {
      if (i >= lineBytes.size) {
        Thread.sleep(100000)
        i = 0
      }

      val data = deserializer.deserialize(lineBytes(i))
      ctx.collect(data)
      rows += 1

      i += step

      try {
        Thread.sleep(sleep)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def cancel(): Unit = {
    this.stop = true
  }
}
