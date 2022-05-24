package scala.sql.timeattr

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.types.Row

/**
 * 和1.12一样。
 * 我靠：这个窗口时间的显示rstTable.execute().print()和rstTable.toAppendStream[Row]时间的显示不一样
 * table的tostring就是当前的时区时间，toAppendStream[Row]后和1.12一样不是当前的时区，之后看看区别
 *
 * sql中定义事件时间的三种方式: https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/concepts/time_attributes/#event-time
 * 定义事件时间一般也就只会用到两种, 直接在最后一列额外声明一个处理时间字段即可, 形式是固定的:
 *    在创建表的 DDL 中定义, 格式如像: WATERMARK FOR ts AS ts - INTERVAL '5' SECOND, 指定存在的列为事件时间并生成延时5s的水位线
 *    在 DataStream 到 Table 转换时定义, 格式如像: 'eventTime.rowtime() as 'event_time, Watermark需要在DataStream中定义好
 */
object SqlDefineEventTimeUseDataStreamTest {

  def main(args: Array[String]): Unit = {
    // 这个就是之前DataStream的env
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
    val sql =
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
