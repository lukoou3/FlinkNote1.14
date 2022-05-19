package scala.connector.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.serialization.SerializationSchemaLogWrapper
import scala.connector.log.LogSinkFunction
import scala.sql.utils.TableImplicits._

class TableSinkJsonSuite extends AnyFunSuite with BeforeAndAfterAll{
  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    // Flink 1.14后，旧的planner被移除了，默认就是BlinkPlanner
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    tEnv = StreamTableEnvironment.create(env, settings)

    /**
     * {"id":"1","name":"罗隐32","age":1300}
     * {"id":"1", "name":"罗隐", "age":30}
     * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
     */
    val sql = """
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
  }

  test("RowDataSinkJson"){
    val sql = """
    select
        id,
        name,
        age,
        current_timestamp ts1,
        localtimestamp ts2,
        count(1) cnt
    from tmp_tb1
    group by id, name, age
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.toRetractStreamOnlyAdd[RowData].addSink(
      // new LogSinkFunction("warn", new SerializationSchemaLogWrapper(rstTable.getJsonRowDataSerializationSchema))
      new LogSinkFunction("warn", rstTable.getJsonRowDataSerializationSchema.wrapLog)
    )
  }

  test("RowSinkJson"){
    val sql = """
    select
        id,
        name,
        age,
        -- Java 8 date/time type `java.time.Instant` not supported by default: add Module "org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype:jackson-datatype-jsr310" to enable handling
        -- current_timestamp ts1,
        -- 2022-05-17T17:49:20.907Z，也有问题，最好用RowData，flinksql官方connector使用的类型
        localtimestamp ts2,
        count(1) cnt
    from tmp_tb1
    group by id, name, age
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.toRetractStreamOnlyAdd[Row].addSink(
      new LogSinkFunction("warn", rstTable.getJsonRowSerializationSchema.wrapLog)
    )
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
