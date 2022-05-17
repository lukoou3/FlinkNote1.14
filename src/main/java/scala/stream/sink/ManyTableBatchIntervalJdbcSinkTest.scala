package scala.stream.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.immutable
import scala.stream.sink.JdbcSinks.{JdbcConnectionOptions, JdbcSinkData, TableSave}


object ManyTableBatchIntervalJdbcSinkTest {

  case class PeopleData(code:Int, name: String, age: Int) extends JdbcSinkData
  case class StudentData(id:Int, name: String, age: Int) extends JdbcSinkData

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val lines =
      """
34,df,0
12,aa46,23
13,苏流沙46,20
14,沐璇音46,19
    """
    val ds: DataStream[(Int, String, Int)] = text.flatMap {
      line =>
        try {
          val words = line.split(",")
          if (words.length >= 3) {
            val code = words(0).toInt
            val name = words(1)
            val age = words(2).toInt
            Some(code, name, age)
          } else {
            None
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }

    val ds1: DataStream[JdbcSinkData] = ds.map(x => PeopleData(x._1, x._2, x._3))
    val ds2: DataStream[JdbcSinkData] = ds.map(x => StudentData(x._1, x._2, x._3))

    val map:Serializable = HashMap("1" -> 1)

    // Caused by: java.io.NotSerializableException: scala.collection.immutable.MapLike$$anon$2
    ds1.union(ds2).addSink(new ManyTableBatchIntervalJdbcSink(
      Map(
        classOf[PeopleData] -> new TableSave[PeopleData]("""insert into people(code,name,age) values(?,?,?)
        on duplicate key update code=VALUES(code),name=VALUES(name),age=VALUES(age)
        """, (stmt, data) => {
        stmt.setInt(1, data.code)
        stmt.setString(2, data.name)
        stmt.setInt(3, data.age)
      }),
        classOf[StudentData] -> new TableSave[StudentData]("""insert into student(id,name,age) values(?,?,?)
        on duplicate key update id=VALUES(id),name=VALUES(name),age=VALUES(age)
        """, (stmt, data) => {
          stmt.setInt(1, data.id)
          stmt.setString(2, data.name)
          stmt.setInt(3, data.age)
        })
      ),
      JdbcConnectionOptions("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8", "root",
        "123456", "com.mysql.jdbc.Driver"),
      5,
      10000
    ))

    env.execute("ManyTableBatchIntervalJdbcSinkTest")
  }

}
