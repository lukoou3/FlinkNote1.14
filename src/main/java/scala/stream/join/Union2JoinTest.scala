package scala.stream.join

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._

import scala.beans.BeanProperty

/**
 * 这里使用union+keyBy+KeyedProcessFunction实现类似connect加上KeyedCoProcessFunction实现双流join
 * union+keyBy+KeyedProcessFunction应该更加通用，可以实现三流的join
 * 不知道官方的connect,KeyedCoProcessFunction是怎么实现的
 *
 * 实现过程中发现元组中如果包含为null的case class的属性时会序列化失败，使用Option没问题
 */
object Union2JoinTest {

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

    val rst = Unoin2JoinKeyedProcessFunction.union(stream1, stream2)(_.id, _.id)
      .process(new Unoin2JoinKeyedProcessFunction[Int, Data1, Data2])
      .mapWith {
        case (k, a, b) =>
          val data1 = a.getOrElse(null)
          val data2 = b.getOrElse(null)
          if (data1 != null && data2 != null) {
          } else if (data1 != null) {
          } else if (data2 != null) {
          }
          (k, a, b)
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
