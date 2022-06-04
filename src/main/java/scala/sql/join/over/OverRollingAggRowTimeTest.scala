package scala.sql.join.over

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

object OverRollingAggRowTimeTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    /**
     * {"name":"a" ,"code":1 ,"ts":1}
     * {"name":"a" ,"code":2 , "ts":2}
     * {"name":"a" ,"code":3 ,"ts":3}
     * {"name":"a" ,"code":4 , "ts":4}
     * {"name":"a" ,"code":5 , "ts":5}
     * {"name":"a" ,"code":6 , "ts":6}
     * {"name":"a" ,"code":6 , "ts":7}
     * {"name":"a" ,"code":6 , "ts":1}
     * {"name":"a" ,"code":6 , "ts":7}
     * {"name":"a" ,"code":6 , "ts":8}
     * {"name":"a" ,"code":6 , "ts":80}
     * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
     */
    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      code int,
      ts bigint,
      tm as to_timestamp(from_unixtime(ts)),
      watermark for tm as tm - interval '5' seconds
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
        name,
        code,
        tm,
        count(1) over last_10second cnt,
        max(code) over last_10second max_code,
        collect(code) over last_10second codes,
        collect(distinct code) over last_10second codes2,
        max(cast(tm as string)) over last_10second max_tm,
        max(substr(cast(tm as string), 1, 10)) over last_10second max_dt
    from tmp_tb1
    window last_10second as (
        partition by name
        order by tm
        range between interval '10' second preceding and current row
    )
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.execute().print()

    env.execute("OverRollingAggTest")
  }

}
