package scala.sql.types

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.connector.localfile.LocalFileSourceFunction
import scala.serialization.BinarySchema

class ByteArrayDs2TableSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  test("json"){
    import scala.sql.utils.TableImplicits.DataStreamTableOps
    val ds = env.addSource(new LocalFileSourceFunction[Array[Byte]]("D:\\IdeaWorkspace\\FlinkNote\\Flink12\\test_data\\online_log.json",
      sleep = 1000, deserializer = new BinarySchema))

    val fields ="""
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
     """
    ds.createTemporaryViewUseJsonFormat(tEnv, "tmp_tb", s"row<$fields>")

    val sql = "select * from tmp_tb"

    tEnv.sqlQuery(sql).execute().print()


  }


  override protected def afterAll(): Unit = {
    env.execute()
  }
}
