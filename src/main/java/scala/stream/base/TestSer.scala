package scala.stream.base

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream, ObjectOutputStream}
import java.net.URLClassLoader

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.data.RowData

object TestSer {

  def main(args: Array[String]): Unit = {
    val parentLoader = new URLClassLoader(List(new File("").toURI.toURL).toArray, null)

    val func = new RichSinkFunction[RowData] {
      //val serializer = _serializer
      override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
        //val bytes = serializer.serialize(value)
        println(11)
      }
    }

    val ser = JavaSerializerInstance.serialize(func)
    println( JavaSerializerInstance.deserialize[RichSinkFunction[_]](ser) )
  }

  object JavaSerializerInstance {

    def serialize[T](t: T): Array[Byte] = {
      val bos = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(bos)
      try {
        objOut.writeObject(t)
      } finally {
        objOut.close()
      }
      bos.toByteArray
    }

    def deserialize[T](bytes: Array[Byte]): T = {
      val bis = new ByteArrayInputStream(bytes)
      val objIn = new ObjectInputStream(bis)
      try {
        objIn.readObject().asInstanceOf[T]
      } finally {
        objIn.close()
      }
    }
  }
}
