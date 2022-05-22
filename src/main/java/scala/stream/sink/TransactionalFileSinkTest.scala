package scala.stream.sink

import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

object TransactionalFileSinkTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.port", "8082")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    env.enableCheckpointing(1000 * 30)
    env.setStateBackend(new HashMapStateBackend())
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointStorage("file:///F:/flink-checkpoints")

    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val datas: DataStream[(String, Long, String)] = text.map { line =>
      val time = System.currentTimeMillis() + 60 * 60 * 1000
      (fmt.format(new Date(time)), time, line)
    }

    val rstDs = datas.map(_.toString())

    // write to files with a transactional sink.
    // results are committed when a checkpoint is completed.
    val (targetDir, transactionDir) = createAndGetPaths
    rstDs.addSink(new TransactionalFileSink[String](targetDir, transactionDir))

    env.execute("TransactionalFileSinkTest")
  }

  /** Creates temporary paths for the output of the transactional file sink. */
  def createAndGetPaths: (String, String) = {
    //val tempDir = System.getProperty("java.io.tmpdir")
    val tempDir = "F:\\flink-fileSink\\TransactionalFileSink"
    val targetDir = s"$tempDir/committed"
    val transactionDir = s"$tempDir/transaction"

    val targetPath = Paths.get(targetDir)
    val transactionPath = Paths.get(transactionDir)

    if (!Files.exists(targetPath)) {
      Files.createDirectory(targetPath)
    }
    if (!Files.exists(transactionPath)) {
      Files.createDirectory(transactionPath)
    }

    (targetDir, transactionDir)
  }

}


