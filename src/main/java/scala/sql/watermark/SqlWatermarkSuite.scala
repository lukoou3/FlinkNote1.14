package scala.sql.watermark

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SqlWatermarkSuite extends AnyFunSuite with BeforeAndAfterAll{
  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    tEnv = StreamTableEnvironment.create(env, settings)
  }

  /**
   * [[org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator]]
   * 生成的watermark就是事件时间 - sql中配置的延时, "watermark for tm as tm - interval '0' seconds"就是直接把当前时间作为watermark
   * 这个和core有点区别，stream默认会延时 1 ms
   * 就算配置的没有延时，在当前记录直接打印当前watermark也不会和当前记录相等，因为watermark是定时生成的
   */
  test("watermark"){
    /**
     * {"name":"a" ,"code":1 ,"ts":1}
     * {"name":"a" ,"code":2 , "ts":2}
     * {"name":"a" ,"code":3 ,"ts":3}
     * {"name":"a" ,"code":4 , "ts":4}
     * {"name":"a" ,"code":5 , "ts":5}
     * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
     */
    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      code int,
      ts bigint,
      tm as to_timestamp(from_unixtime(ts)),
      watermark for tm as tm - interval '0' seconds
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
        *,
        current_watermark(tm) wm
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }


  override protected def afterAll(): Unit = {
    env.execute()
  }
}
