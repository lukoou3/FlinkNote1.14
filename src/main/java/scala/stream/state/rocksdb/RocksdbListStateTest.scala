package scala.stream.state.rocksdb

import java.sql.Timestamp

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/state/state_backends/
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/ops/state/state_backends/#memorystatebackend
 * 和HashMapStateBackend使用表面上是一样的
 * 当状态比较大使用ListState/MapState，吞吐量不大时使用比较适合。每次读写文件数据库涉及到序列化和反序列化
 */
object RocksdbListStateTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()

    env.enableCheckpointing(1000 * 60 * 3)
    env.setStateBackend(new EmbeddedRocksDBStateBackend())
    env.getCheckpointConfig.setCheckpointStorage("file:///F:/flink-checkpoints")
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val actDs = text.flatMap {
      line =>
        val array = line.trim.split("\\s+").filter(_.nonEmpty).map((_, new Timestamp(System.currentTimeMillis()).toString.substring(11, 19)))
        // array.foreach(println)
        array
    }

    val rstDs = actDs.keyBy(_._1).process(new KeyedProcessFunction[String, (String, String), String] {
      var datasState: ListState[(String, String)] = _

      override def open(parameters: Configuration): Unit = {
        val ttlConfig = StateTtlConfig
          .newBuilder(Time.seconds(10))
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          .build
        val stateDescriptor = new ListStateDescriptor[(String, String)]("datas-state", createTypeInformation[(String, String)])
        stateDescriptor.enableTimeToLive(ttlConfig)
        datasState = getRuntimeContext.getListState(stateDescriptor)
      }

      override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, out: Collector[String]): Unit = {
        datasState.add(value)
        val iterable = datasState.get()

        val times = iterable.asScala.toIterator.map(_._2).mkString(",")

        out.collect(value._1 + "：" + times)
      }
    })

    rstDs.addSink(println(_))

    env.execute("RocksdbListStateTest")
  }

}
