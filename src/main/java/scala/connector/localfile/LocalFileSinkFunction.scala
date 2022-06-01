package scala.connector.localfile

import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets

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
    // println(getRuntimeContext.getNumberOfParallelSubtasks, getRuntimeContext.getIndexOfThisSubtask)
    /*if(getRuntimeContext.getNumberOfParallelSubtasks != 1){
      throw new Exception("this operate Parallel must is 1")
    }*/
    serializer.open(RuntimeContextInitializationContextAdapters.serializationAdapter(getRuntimeContext()))
    val filePath = if(getRuntimeContext.getNumberOfParallelSubtasks == 1){
      path
    }else{
      val i = getRuntimeContext.getIndexOfThisSubtask
      path.split("""\.""").toList match {
        case x :+ last => if(x.isEmpty) last + i else x.mkString(".") + i + last
      }
    }
    file = new RandomAccessFile(filePath, "rw")
    file.seek(file.length())
  }

  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    val body = serializer.serialize(value)
    // val line = new String(body, StandardCharsets.UTF_8)
    // println(line)
    file.write(body)
    file.write(NEW_LINE)
  }

  override def close(): Unit = {
    if(file != null){
      file.close()
    }
  }

}
