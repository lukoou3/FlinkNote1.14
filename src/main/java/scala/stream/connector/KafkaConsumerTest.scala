package scala.stream.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.log.Logging

/**
 * 这个api被标记过时了，1.15版本会被移除。
 * FlinkKafkaConsumer is deprecated and will be removed with Flink 1.15, please use KafkaSource instead.
 */
@deprecated
object KafkaConsumerTest extends Logging {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    //val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("group.id", "test-group")

    val kafkaConsumer = new FlinkKafkaConsumer("online-log", new SimpleStringSchema(), properties);

    val ds = env.addSource(kafkaConsumer)

    ds.print()

    env.execute("KafkaConsumerTest")
  }

}
