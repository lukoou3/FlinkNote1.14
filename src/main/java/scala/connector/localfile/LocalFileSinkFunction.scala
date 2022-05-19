package scala.connector.localfile

import java.io.RandomAccessFile

import org.apache.flink.api.common.serialization.{RuntimeContextInitializationContextAdapters, SerializationSchema}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.log.Logging

// 仅仅用于测试
class LocalFileSinkFunction[T](
  path: String,
  serializer: SerializationSchema[T]
) extends RichSinkFunction[T] with Logging{
  @transient var file: RandomAccessFile = _
  val NEW_LINE = '\n'.toByte

  override def open(parameters: Configuration): Unit = {
    println(getRuntimeContext.getNumberOfParallelSubtasks, getRuntimeContext.getIndexOfThisSubtask)
    if(getRuntimeContext.getNumberOfParallelSubtasks != 1){
      throw new Exception("this operate Parallel must is 1")
    }
    serializer.open(RuntimeContextInitializationContextAdapters.serializationAdapter(getRuntimeContext()))
    file = new RandomAccessFile(path, "rw")
    file.seek(file.length())
  }

  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    val body = serializer.serialize(value)
    file.write(body)
    file.write(NEW_LINE)
  }

  override def close(): Unit = {
    if(file != null){
      file.close()
    }
  }

}
