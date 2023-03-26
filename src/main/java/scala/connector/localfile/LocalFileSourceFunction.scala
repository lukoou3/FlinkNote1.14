package scala.connector.localfile

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.{DeserializationSchema, RuntimeContextInitializationContextAdapters}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.log.Logging

class LocalFileSourceFunction[T](
  filePath: String,
  sleep: Long = 10,
  numberOfRowsForSubtask: Long = Long.MaxValue,
  cycleNum: Int = Int.MaxValue,
  deserializer: DeserializationSchema[T]
) extends RichParallelSourceFunction[T] with Logging {
  var stop = false
  //val lineBytes = ArrayBuffer[Array[Byte]]()

  override def open(parameters: Configuration): Unit = {
    deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()))
    /*Source.fromFile(filePath, "utf-8").getLines().filter(_.trim != "").slice(0, 10000).foreach{ line =>
      lineBytes += line.getBytes(StandardCharsets.UTF_8)
    }*/
  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    var rows = 0
    var cycles = 0

    while (!stop && rows < numberOfRowsForSubtask && cycles < cycleNum) {
      // 能读取大文件
      for (line <- Source.fromFile(filePath, "utf-8").getLines().filter(_.trim != "") if rows < numberOfRowsForSubtask) {
        val lineByte = line.getBytes(StandardCharsets.UTF_8)
        val data = deserializer.deserialize(lineByte)
        ctx.collect(data)
        rows += 1

        try {
          if(sleep > 0){
            Thread.sleep(sleep)
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }

      cycles += 1
      /*if(cycles < cycleNum && rows < numberOfRowsForSubtask){
        Thread.sleep(100000)
      }*/
    }
  }

  def cancel(): Unit = {
    this.stop = true
  }
}
