package scala.sql.types

import java.nio.charset.StandardCharsets

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.data.RowData
import org.apache.flink.types.{Row, RowKind}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.sql.utils.TableImplicits._

class TemporalFunctionSuite extends AnyFunSuite with BeforeAndAfterAll {
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

  test("interval") {
    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        interval '5' seconds a,
        interval '1' days b,
        interval '10 00:00:00.004' day to seconds c,
        -- 就这两种类型
        interval '2' month d,
        interval '2' year e,
        interval '2-10' year to month f,
        *
    from tmp_tb1
    """
    val table = tEnv.sqlQuery(sql)
    val resolvedSchema: ResolvedSchema = table.getResolvedSchema
    table.printSchema()

    val rowDataDs: DataStream[RowData] = scala.connector.common.Utils.getRowDataDataStreamInternal(table, null)
    println(rowDataDs.dataType)

    val serializer = table.getJsonRowDataSerializationSchema
    rowDataDs.addSink{row =>
      println(row.getRowKind.shortString(), new String(serializer.serialize(row), StandardCharsets.UTF_8))
    }
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
