package scala.connector.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.OrcFile
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CsvFormatSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  test("tmp_csv"){
    var sql1 =
      """
    create temporary table tmp_csv (
      id bigint,
      name string,
      age int
    ) WITH (
      'connector' = 'filesystem',
      'path' = 'file:///D:\ideaProjects\FlinkNote1.14\files\test_csv.txt',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true',
      'csv.field-delimiter' = ','
    )
    """
    var sql2 = """
    create temporary table tmp_csv (
        id bigint,
        name string,
        age int
    ) with (
      'connector' = 'mylocalfile',
      'path' = 'D:\ideaProjects\FlinkNote1.14\files\test_csv.txt',
      'number-of-rows-subtask' = '9',
      'source.sleep' = '10',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true',
      'csv.field-delimiter' = ','
    )
    """
    tEnv.executeSql(sql1)

    val sql = """
    select
        *
    from tmp_csv
    """

    val rstTable = tEnv.sqlQuery(sql)

    rstTable.toDataStream.addSink{row =>
      println(row)
    }
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }

}
