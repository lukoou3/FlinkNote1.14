package scala.sql.tvf

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala._

import scala.sql.timeattr.{OnlineLog, OnlineLogSouce}

object WindowTvfProcessingTimeTumbleTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.port", "8082")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 每个分区：每秒1个。整个应用：每秒2个，5秒10个
    val onlineLog: DataStream[OnlineLog] = env.addSource(new OnlineLogSouce(count = 1, sleepMillis = 1000, pageNum=1))
    println("Source parallelism:" + onlineLog.parallelism)

    // 声明一个额外的字段作为处理时间属性字段
    // 使用case类时，定义的列可以选择自定义顺序, 使用元组就不行
    // tEnv.createTemporaryView("tmp_tb", onlineLog, 'timeStr as 'ptime, 'pageId as 'page_id, 'visitCnt as 'cnt, 'ts.proctime)
    tEnv.createTemporaryView("tmp_tb",
      tEnv.fromDataStream(onlineLog,'timeStr as 'ptime, 'pageId as 'page_id, 'visitCnt as 'cnt, 'ts.proctime)
    )
    // 不行，proctime必须定义在table生成之前
    //tEnv.createTemporaryView("tmp_tb", tEnv.fromDataStream(onlineLog).select('timeStr as 'ptime, 'pageId as 'page_id, 'visitCnt as 'cnt, 'ts.proctime))

    /**
     * 这两个sql实现的效果一样，第一个是1.13之前的实现，第二个是新的tvf语法
     * tvf能使用的功能更多
     */
    var sql =
      """
    select
        TUMBLE_START(ts, INTERVAL '5' SECOND) as wstart,
        TUMBLE_END(ts, INTERVAL '5' SECOND) as wend,
        min(ptime) min_ptime,
        max(ptime) max_ptime,
        page_id,
        sum(cnt) pv
    from tmp_tb
    group by page_id, TUMBLE(ts, INTERVAL '5' SECOND)
    """
    sql =
      """
    select
        window_start wstart,
        window_end wend,
        min(ptime) min_ptime,
        max(ptime) max_ptime,
        page_id,
        sum(cnt) pv
    from table( tumble(table tmp_tb, descriptor(ts), interval '5' second) )
    group by window_start, window_end, page_id
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    //rstTable.toAppendStream[Row].print()
    rstTable.execute().print()

    env.execute()
  }

}
