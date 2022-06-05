package scala.connector.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

import scala.sql.udf.internal.JsonTuple


object RawFormatTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    tEnv.createTemporarySystemFunction("json_tuple", new JsonTuple)

    var sql = """
    create table tmp_tb1 (
      line string
    ) with (
      'connector' = 'mylocalfile',
      'path' = 'D:\IdeaWorkspace\FlinkNote\Flink12\test_data\online_log.json',
      'source.sleep' = '1000',
      'format' = 'raw'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        line,
        dt,
        bs,
        report_time,
        os_plant,
        page_id,
        page_name,
        page_param
    from tmp_tb1
    left join lateral table(json_tuple(line, 'dt', 'bs', 'report_time', 'os_plant', 'page_id', 'page_name', 'page_param')) on true
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.execute().print()

    env.execute("RawFormatTest")
  }

}
