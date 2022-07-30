package scala.stream.func

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import BatchIntervalFlatMapOperator.DataStreamBatchIntervalFlatMapOps
import scala.log.Logging

class BatchIntervalFlatMapSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  test("a"){
    //env.setParallelism(2)
    env.enableCheckpointing(1000 * 60)
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(5), Time.seconds(3)))

    val text: DataStream[String] = env.socketTextStream("localhost", 9999).map(identity(_))

    //text.batchIntervalFlatMap3(3, 1000 * 10L)( x => Option(x.mkString(",")))
    text.batchIntervalMap2(3, 1000 * 10L)( x => x.mkString(",")).addSink(new RichSinkFunction[String] with Logging {
      override def open(parameters: Configuration): Unit = {
        logWarning("sink open")
      }

      override def invoke(value: String, context: SinkFunction.Context): Unit = {
        println(value)
      }

      override def close(): Unit = {
        logWarning("sink close")
      }

    })
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
