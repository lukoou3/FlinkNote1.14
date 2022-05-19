package scala.sql.table.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData

import scala.sql.utils.TableImplicits._

import scala.sql.table.localfile.LocalFileSinkFunction

object LocalFileSinkFunctionTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()

    // Flink 1.14后，旧的planner被移除了，默认就是BlinkPlanner
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    /**
     * {"id":"1","name":"罗隐32","age":1300}
     * {"id":"1", "name":"罗隐", "age":30}
     * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
     */
    var sql = """
    CREATE TABLE tmp_tb1 (
      id int,
      name string,
      age int
      -- PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
      'connector' = 'mysocket',
      'hostname' = 'localhost',
      'port' = '9999',
      'format' = 'json',
      -- format的参数配置，前面需要加format的名称
      'json.fail-on-missing-field' = 'false',
      -- json解析报错会直接返回null(row是null), 没法跳过忽略, {}不会报错, 属性都是null
      'json.ignore-parse-errors' = 'true'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id,
        name,
        age
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.toAppendStream[RowData].addSink(new LocalFileSinkFunction(
      "D:\\flink-fileSink\\aaa\\aaa.txt",
      rstTable.getJsonRowDataSerializationSchema
    )).setParallelism(1)


    env.execute("LocalFileSinkFunctionTest")
  }

}
