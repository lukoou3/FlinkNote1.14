package scala.stream.join

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._

/**
 * 测试自定义实现的3流join的函数，使用scala实现，可以利用隐士转换少传递很多参数，简化了很多
 * 只要在固定的时间join上就能merg输出，关联不上的也会输出
 * 时间参数也支持配置
 */
object Union3JoinTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(4)

    val text1: DataStream[String] = env.socketTextStream("localhost", 9999)
    val text2: DataStream[String] = env.socketTextStream("localhost", 9988)
    val text3: DataStream[String] = env.socketTextStream("localhost", 9966)
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

    val stream3 = text3.flatMap {
      line =>
        try {
          val arrays = line.trim.split("\\s+")
          if (arrays.length >= 2) {
            val id = arrays(0).toInt
            val score = arrays(1).toDouble
            println(s"source:$line")
            Some(Data3(id, score))
          } else None
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }

    val rst = stream1.map(x => (x.id, Option(x), None:Option[Data2], None:Option[Data3]))
      .union(
        stream2.map(x => (x.id, None, Option(x), None)),
        stream3.map(x => (x.id, None, None, Option(x)))
      )
      .keyBy(_._1)
      .process(new Unoin3JoinKeyedProcessFunction[Int, Data1, Data2, Data3])
      .mapWith {
        case (k, a, b, c) =>
          val data1 = a.getOrElse(null)
          val data2 = b.getOrElse(null)
          val data3 = c.getOrElse(null)
          if (data1 != null && data2 != null && data3 != null) {
          } else if (data1 != null) {
          } else if (data2 != null) {
          }else if (data3 != null) {
          }
          (k, a, b, c)
      }

    rst.print()

    env.execute("Union3JoinTest")
  }

  case class Data1(id: Int, name: String)
  case class Data2(id: Int, age: Int)
  case class Data3(id: Int, score: Double)

}
