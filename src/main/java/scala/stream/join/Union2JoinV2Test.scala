package scala.stream.join

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._

import scala.beans.BeanProperty

/**
 *
 */
object Union2JoinV2Test {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(4)

    val text1: DataStream[String] = env.socketTextStream("localhost", 9999)
    val text2: DataStream[String] = env.socketTextStream("localhost", 9988)
    val stream1 = text1.flatMap {
      line =>
        try {
          val arrays = line.trim.split("\\s+")
          if (arrays.length >= 2) {
            val id = arrays(0).toInt
            val name = arrays(1)
            println(s"source:$line")
            Some(Data1(id, name))
          } else None
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }

    val stream2 = text2.flatMap {
      line =>
        try {
          val arrays = line.trim.split("\\s+")
          if (arrays.length >= 2) {
            val id = arrays(0).toInt
            val age = arrays(1).toInt
            println(s"source:$line")
            Some(Data2(id, age))
          } else None
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }

    val rst = Unoin2JoinKeyedProcessFunctionV2.union(stream1, stream2)(_.id, _.id)
      .process(new Unoin2JoinKeyedProcessFunctionV2[Int, Data1, Data2])
      .map{ data =>
          val key = data.key
          val data1 = data.data1
          val data2 = data.data2
          if (data1 != null && data2 != null) {
          } else if (data1 != null) {
          } else if (data2 != null) {
          }
          (key, data1, data2)
      }

    rst.print()

    env.execute("WordCountTumblingWindow")
  }

  class DataBean {
    @BeanProperty
    var id: Int = _
    @BeanProperty
    var name: String = _

    def this(id: Int, name: String) = {
      this()
      this.id = id
      this.name = name
    }
  }

  case class Data1(id: Int, name: String)

  case class Data2(id: Int, age: Int)

}
