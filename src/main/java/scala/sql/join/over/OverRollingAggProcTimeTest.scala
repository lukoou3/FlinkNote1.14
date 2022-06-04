package scala.sql.join.over

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * [[org.apache.flink.table.runtime.operators.over.ProcTimeRangeBoundedPrecedingFunction]]
 * 聚合函数生成：org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator#generateAggsHandler(java.lang.String, org.apache.flink.table.planner.plan.utils.AggregateInfoList)
 *    org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction
 *    org.apache.flink.table.planner.functions.aggfunctions.CollectAggFunction
 * 在over window中使用collect(distinct code)没问题，虽然聚合的CollectAggFunction每个key的值只有1，而且也有增加减少的操作，
 * 但是实际在生成的BoundedOverAggregateHelper中有计数的判断
 */
object OverRollingAggProcTimeTest {

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
        -- MULTISET<INT>, 就是一个集合，就是会对元素集合，相当于list
        collect(code) over last_10second codes,
        collect(distinct code) over last_10second codes2,
        listagg(cast(code as string)) over last_10second codes_str,
        listagg(cast(code as string), '|') over last_10second codes_str2,
        max(cast(proctime as string)) over last_10second max_proctime,
        max(cast(tm as string)) over last_10second max_tm
    from tmp_tb1
    window last_10second as (
        partition by name
        order by proctime
        range between interval '10' second preceding and current row
    )
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    //rstTable.execute().print()
    rstTable.toAppendStream[Row].addSink{row =>
      println(row)
    }

    env.execute("OverRollingAggTest")
  }

}
