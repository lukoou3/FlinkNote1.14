package scala.stream.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer

/**
 * JdbcSink表现和flink1.12用法一样
 * 一个优化是可以配置enableObjectReuse，内部可根据情况复制对象：copyIfNecessary方法。
 * JdbcOutputFormat继承了InputTypeConfigurable，addSink会回调设置TypeInformation
 */
object JdbcSinkTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 1.14没这个限制了
    env.getConfig.enableObjectReuse()

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val lines =
      """
34,df,0,2021-03-11 01:47:04
12,aa,23,2021-03-11 01:46:48
41,苏流沙,20,1992-02-18 18:30:10
31,沐璇音,19,1993-06-12 09:00:20
    """
    val peopleDS: DataStream[(Int, String, Int, String)] = text.flatMap {
      line =>
        try {
          val words = line.split(",")
          if (words.length >= 4) {
            val code = words(0).toInt
            val name = words(1)
            val age = words(2).toInt
            val birthday = words(3)
            Some(code, name, age, birthday)
          } else {
            None
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }

    peopleDS.addSink(JdbcSinkScala.sink[(Int, String, Int, String)](
      """insert into people(code,name,age,birthday) values(?,?,?,?)
        on duplicate key update code=VALUES(code),name=VALUES(name),age=VALUES(age),birthday=VALUES(birthday)
        """,
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8")
        .withDriverName("com.mysql.jdbc.Driver")
        .withUsername("root").withPassword("123456")
        .build(),
      JdbcExecutionOptions.builder()
        .withBatchSize(3)
        .withBatchIntervalMs(30000)
        .build()
    )((ps, e) => {
      val (code, name, age, birthday) = e
      ps.setInt(1, code)
      ps.setString(2, name)
      ps.setInt(3, age)
      ps.setString(4, birthday)
    })).setParallelism(1)


    env.execute("JdbcSinkTest")
  }

}
