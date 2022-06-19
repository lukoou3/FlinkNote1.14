package scala.sql.watermark

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.sql.timeattr.OnlineLog

class SteamWatermarkSuite extends AnyFunSuite with BeforeAndAfterAll{
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
   * [[org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks]]
   * 初始的: maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1
   * 之后每来一个元素: maxTimestamp = Math.max(maxTimestamp, eventTimestamp)
   * 定期生成watermark: maxTimestamp - outOfOrdernessMillis - 1
   * stream默认会延时 1 ms, sql的配置0时不会减去这个1
   *
   * [[org.apache.flink.streaming.runtime.operators.TimestampsAndWatermarksOperator]]
   * 默认每200ms生成watermark
   */
  test("watermark"){
    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text = env.socketTextStream("localhost", 9999)
    val ds1 = text.map(_.toLong).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[Long](Duration.ofSeconds(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Long] {
          override def extractTimestamp(element: Long, recordTimestamp: Long): Long = {
            element
          }
        })
    )
    //val ds2 = text.map(_.toLong).assignAscendingTimestamps(identity)

    ds1.addSink(new RichSinkFunction[Long] {
      override def invoke(value: Long, context: SinkFunction.Context): Unit = {
        println(value, context.currentWatermark())
      }
    })
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
