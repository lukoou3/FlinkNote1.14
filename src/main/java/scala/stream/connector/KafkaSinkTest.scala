package scala.stream.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaRecordSerializationSchemaBuilder, KafkaSink}
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.scala._

import scala.connector.localfile.LocalFileSourceFunction

object KafkaSinkTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    // val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    env.enableCheckpointing(1000 * 30)

    /**
     * 和1.12这样设置效果一样, 可以看FsStateBackend类的注释
     * env.setStateBackend(new FsStateBackend("file:///F:/flink-checkpoints"))
     */
    env.setStateBackend(new HashMapStateBackend())
    env.getCheckpointConfig.setCheckpointStorage("file:///F:/flink-checkpoints")

    val onlineLog = env.addSource(
      new LocalFileSourceFunction("D:\\IdeaWorkspace\\FlinkNote\\Flink12\\test_data\\online_log.json",
        sleep = 100, deserializer = new SimpleStringSchema())
      //new LocalFileSourceFunction("D:\\\\ideaProjects\\FlinkNote\\Flink12\\test_data\\online_log.json", sleep = 100)
    )

    val brokers = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    val sink = KafkaSink.builder[String]()
      .setBootstrapServers(brokers)
      .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder()
        .setTopic("online-log")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setKafkaProducerConfig(new Properties)
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build();

    onlineLog.sinkTo(sink)

    env.execute("KafkaSinkTest")
  }

}
