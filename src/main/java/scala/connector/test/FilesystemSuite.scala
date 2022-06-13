package scala.connector.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.OrcFile
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class FilesystemSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  test("filesystem"){
    var sql =
      """
    CREATE TABLE tmp_tb(
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
    ) WITH (
      'connector' = 'filesystem',
      'path' = 'file:///D:\IdeaWorkspace\FlinkNote\Flink12\test_data\online_log.json',
      'format' = 'json',
      -- format的参数配置，前面需要加format的名称
      'json.fail-on-missing-field' = 'false',
      -- json解析报错会直接返回null(row是null), 没法跳过忽略, {}不会报错, 属性都是null
      'json.ignore-parse-errors' = 'true'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        *
    from tmp_tb
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.toDataStream.addSink{row =>
      println(row)
    }
  }

  test("FileSystem"){
    val path = """file:///D:\IdeaWorkspace\FlinkNote\Flink12\test_data\online_log.json"""
    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration)
    val fsPath = new Path(path)
    if(fs.exists(fsPath)){
      val inputStream = fs.open(fsPath)
      for (line <- Source.fromInputStream(inputStream, "utf-8").getLines()) {
        println(line)
      }
    }
  }

  test("FileSystem-orc"){
    val path = "file:///D:/chromedownload/pin_ord_stat.snappy.orc"
    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration)
    val fsPath = new Path(path)
    if(fs.exists(fsPath)){
      val conf = new org.apache.hadoop.conf.Configuration
      val reader = OrcFile.createReader(
        fsPath,
        OrcFile.readerOptions(conf).filesystem(fs)
      )

      val rows = reader.rows
      val batch = reader.getSchema.createRowBatch(10240)

      println(reader.getSchema)

      while (rows.nextBatch(batch)) {
        /*for(i <- 0 until batch.size){
        }*/
        println(batch.cols.toList)
        //println(batch.toString)
      }

      rows.close()
    }
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
