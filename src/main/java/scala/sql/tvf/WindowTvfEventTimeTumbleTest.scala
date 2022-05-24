package scala.sql.tvf

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

import scala.sql.timeattr.{OnlineLog, OnlineLogSouce}

object WindowTvfEventTimeTumbleTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 每秒2个，5秒10个
    env.setParallelism(2)

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 每个分区：每秒1个。整个应用：每秒2个，5秒10个。使用事件时间, 实际2s, 事件时间1s, 5秒20个。
    val onlineLog: DataStream[OnlineLog] = env.addSource(new OnlineLogSouce(count = 1, sleepMillis = 1000, pageNum=1)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[OnlineLog](Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[OnlineLog] {
          override def extractTimestamp(element: OnlineLog, recordTimestamp: Long): Long = {
            element.eventTime
          }
        })
    )
    println("Source parallelism:" + onlineLog.parallelism)

    // eventTime字段已经用作事件时间抽取了，不用再用一个新字段来表示事件时间了
    // 使用case类时，定义的列可以选择自定义顺序, 使用元组就不行
    tEnv.createTemporaryView("tmp_tb", onlineLog, 'timeStr as 'ptime, 'eventTimeStr as 'event_time_str,
      'pageId as 'page_id, 'visitCnt as 'cnt, 'eventTime.rowtime() as 'event_time)

    /**
     * 这两个sql实现的效果一样，第一个是1.13之前的实现，第二个是新的tvf语法
     * tvf能使用的功能更多
     */
    var sql =
      """
    select
        TUMBLE_START(event_time, INTERVAL '5' SECOND) as wstart,
        TUMBLE_END(event_time, INTERVAL '5' SECOND) as wend,
        min(event_time_str) min_event_time,
        max(event_time_str) max_event_time,
        -- min(ptime) min_ptime,
        -- max(ptime) max_ptime,
        page_id,
        sum(cnt) pv
    from tmp_tb
    group by page_id, TUMBLE(event_time, INTERVAL '5' SECOND)
    """
    sql =
      """
    select
        window_start wstart,
        window_end wend,
        min(event_time_str) min_event_time,
        max(event_time_str) max_event_time,
        -- min(ptime) min_ptime,
        -- max(ptime) max_ptime,
        page_id,
        sum(cnt) pv
    from table( tumble(table tmp_tb, descriptor(event_time), interval '5' second) )
    group by window_start, window_end, page_id
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()

    rstTable.toAppendStream[Row].addSink(new SinkFunction[Row] {
      override def invoke(row: Row, context: SinkFunction.Context): Unit = {
        println(row)
      }
    })

    env.execute()
  }
}
