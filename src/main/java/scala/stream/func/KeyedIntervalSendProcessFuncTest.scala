package scala.stream.func

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import scala.log.Logging

/**
 * 代码和效果和1.12一样，区别是StateBackend设置api变了(小问题)
 *
 * 经过测试：
 *    isEager = true正常，状态和定时器也能恢复
 *    isEager = false正常，状态和定时器也能恢复
 */
object KeyedIntervalSendProcessFuncTest extends Logging{

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    if (args.length > 0) {
      val checkpointPath = args(0)
      conf.setString("execution.savepoint.path", checkpointPath)
      println(checkpointPath)
    }
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()

    env.enableCheckpointing(1000 * 10)
    /**
     * 和1.12这样设置效果一样, 可以看FsStateBackend类的注释
     * env.setStateBackend(new FsStateBackend("file:///F:/flink-checkpoints"))
     */
    env.setStateBackend(new HashMapStateBackend())
    env.getCheckpointConfig.setCheckpointStorage("file:///F:/flink-checkpoints")
    // 表示下 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint 会在整个作业 Cancel 时被删除。Checkpoint 是作业级别的保存点。
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)
    val startTs = System.currentTimeMillis()

    val words: DataStream[(String, Long)] = text.flatMap {
      line =>
        val array = line.trim.split("\\s+").filter(_.nonEmpty).map((_, System.currentTimeMillis() - startTs))
        array.foreach(println)
        array
    }

    words.keyBy(_._1)
      .process(new KeyedIntervalSendProcessFunc[String, (String, Long)](
        1000 * 10, isEager = false)
      )
      .addSink(new RichSinkFunction[(String, Long)]() {
        override def invoke(value: (String, Long), context: SinkFunction.Context): Unit = {
          val ts = System.currentTimeMillis() - startTs
          println(s"out:$ts: ${ts - value._2}: $value")
        }
      })

    env.execute("KeyedIntervalSendProcessFuncTest")
  }

}
