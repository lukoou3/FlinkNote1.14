package scala.stream.connector

import java.util.Properties

import org.apache.flink.api.common.eventtime.{NoWatermarksGenerator, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer

import scala.stream.util.FlinkJava8Interface

object KafkaSourceTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    /**
     * 蛋疼，消费位置的配置也变了，配置committedOffsets实现之前auto.offset.reset的配置
     * 别配置 OffsetsInitializer.earliest()，这个是空最初的offset消费，不管提交的offset
     *
     * table的配置似乎也变了，EARLIEST匹配的是OffsetsInitializer.earliest()。
     * GROUP_OFFSETS匹配的是OffsetsInitializer.committedOffsets()，没提交的offset会抛出异常
     * [[KafkaDynamicSource#createKafkaSource]]
     */
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("group.id", "test-group")
    properties.setProperty("enable.auto.commit", "true")
    properties.setProperty("auto.commit.interval.ms", "2000")
    val source = KafkaSource.builder[String]()
      .setTopics("online-log")
      .setGroupId("test-group")
      .setProperties(properties)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      //.setStartingOffsets(FlinkJava8Interface.committedOffsetsInitializer(OffsetResetStrategy.EARLIEST))
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    //val ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val ds = env.fromSource(source, new WatermarkStrategy[String]() {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[String] = {
        new NoWatermarksGenerator
      }
    }, "Kafka Source")

    ds.print()

    env.execute("KafkaSourceTest")
  }

}
