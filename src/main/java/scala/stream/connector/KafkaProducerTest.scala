package scala.stream.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import scala.connector.localfile.LocalFileSourceFunction
import scala.log.Logging

/**
 * 这个api被标记过时了，1.15版本会被移除。FlinkKafkaConsumer is deprecated and will be removed with Flink 1.15, please use KafkaSource instead.
 * ./zookeeper_cluster.sh start
 * ./zookeeper_cluster.sh status
 * ./kafka_cluster.sh start
 * xcall jps
 *
 * bin/kafka-topics.sh --zookeeper hadoop101:2181,hadoop102:2181 --create  --replication-factor 2 --partitions 3 --topic online-log
 * bin/kafka-topics.sh --zookeeper hadoop101:2181,hadoop102:2181 --describe --topic online-log
 * bin/kafka-console-consumer.sh --bootstrap-server hadoop101:9092,hadoop102:9092,hadoop103:9092 --from-beginning --topic online-log
 * bin/kafka-consumer-offset-checker.sh --zookeeper hadoop101:2181,hadoop102:2181 --topic online-log --group test-group
 * bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zookeeper hadoop101:2181,hadoop102:2181 --topic online-log --group test-group
 */
@deprecated
object KafkaProducerTest extends Logging{

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    // val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    env.enableCheckpointing(1000 * 30)
    // 这个设置方式也过时了
    env.setStateBackend(new FsStateBackend("file:///F:/flink-checkpoints"))

    val onlineLog = env.addSource(
      new LocalFileSourceFunction("D:\\IdeaWorkspace\\FlinkNote\\Flink12\\test_data\\online_log.json",
        sleep = 100, deserializer = new SimpleStringSchema())
      //new LocalFileSourceFunction("D:\\\\ideaProjects\\FlinkNote\\Flink12\\test_data\\online_log.json", sleep = 100)
    )

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
    properties.setProperty("transaction.timeout.ms", "60000")

    /*val kafkaProducer = new FlinkKafkaProducer[String](
      "online-log", new SimpleStringSchema(),
      properties)*/

    val kafkaProducer = new FlinkKafkaProducer[String](
      "online-log",  new SimpleStringSchema(),
      properties)

    onlineLog.addSink(kafkaProducer)

    env.execute("KafkaProducerTest")
  }

}
