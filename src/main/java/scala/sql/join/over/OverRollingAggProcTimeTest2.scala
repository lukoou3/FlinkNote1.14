package scala.sql.join.over

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * 窗口大小必须一样：
 * Caused by: org.apache.flink.table.api.TableException: Over Agg: Unsupported use of OVER windows.
 *    All aggregates must be computed on the same window. please re-check the over window statement.
 * 想实现多个窗口的聚合，可不可以使用最大的窗口，使用if过滤？
 * 看了下源码，并不能，input每次就确定了要是输入curren_timestamp,元素输入时就会计算好，而不是over聚合时再计算
 * 而且聚合有增量加减的逻辑，也不应该你有这种实现
 * [[org.apache.flink.table.runtime.operators.over.ProcTimeRangeBoundedPrecedingFunction]]
 */
object OverRollingAggProcTimeTest2 {

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
      proctime as proctime(),
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
        proctime,
        tm,
        count(1) over last_10second cnt,
        max(code) over last_10second max_code,
        max(cast(proctime as string)) over last_10second max_proctime,
        max(cast(tm as string)) over last_10second max_tm,
        count(1) over(
          partition by name order by proctime
          range between interval '20' second preceding and current row
        )  cnt2,
        count(1) over(
          partition by name order by proctime
          range between interval '30' second preceding and current row
        )  cnt3
    from tmp_tb1
    window last_10second as (
        partition by name
        order by proctime
        range between interval '10' second preceding and current row
    )
    """
    val rstTable = tEnv.sqlQuery(sql)

    //rstTable.execute().print()
    rstTable.toAppendStream[Row].addSink{row =>
      println(row)
    }

    env.execute("OverRollingAggTest")
  }

}
