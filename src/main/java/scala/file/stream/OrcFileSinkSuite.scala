package scala.file.stream

import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.orc.vector.Vectorizer
import org.apache.flink.orc.writer.OrcBulkWriterFactory
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, LongColumnVector, VectorizedRowBatch}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class OrcFileSinkSuite extends AnyFunSuite with BeforeAndAfterAll{
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
   * 如果停止程序会使生成的.inprogress.文件一直处于未完成状态，虽然hive读不到
   * 还是使用hive sql 插入吧, 应该是在临时目录生成的文件， 然后把文件移到到目录
   */
  test("orc_sink"){
    env.enableCheckpointing(1000 * 60)

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val ds: DataStream[(Int, String)] = env.socketTextStream("localhost", 9999).map{line =>
      (1, line)
    }

    val writerProperties = new Properties()
    writerProperties.setProperty("orc.compress", "SNAPPY")
    val schema: String = "struct<id:int,name:string>"
    val writerFactory = new OrcBulkWriterFactory(new Vectorizer[(Int, String)](schema){
      override def vectorize(element: (Int, String), batch: VectorizedRowBatch): Unit = {
        val row = batch.size
        batch.size += 1
        val idColVector = batch.cols(0).asInstanceOf[LongColumnVector]
        val nameColVector = batch.cols(1).asInstanceOf[BytesColumnVector]
        idColVector.vector(row) = element._1
        nameColVector.setVal(row, element._2.getBytes(StandardCharsets.UTF_8))
      }
    },writerProperties, new org.apache.hadoop.conf.Configuration )


    // StreamingFileSink这个也可以
    val sink = FileSink
      .forBulkFormat(new org.apache.flink.core.fs.Path("file:///F:/flink-fileSink/filesystem"), writerFactory)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()

    ds.sinkTo(sink)
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }

}
