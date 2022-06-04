package scala.sql.udf

import java.nio.charset.StandardCharsets

import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.json.{JsonFormatOptions, JsonRowDataSerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.logical.RowType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.connector.log.LogSinkFunction

class ComplexTypeCreatorSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  test("named_struct"){
    tEnv.createTemporarySystemFunction("named_struct", new CreateNamedStruct)

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      log_time timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        age,
        cnt,
        named_struct('name', name, 'age', age, 'cnt', cnt, 'log_time', log_time) info
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    //rstTable.execute().print()

    val rstDs = rstTable.toAppendStream[RowData]
    val rowType = rstTable.getResolvedSchema.toPhysicalRowDataType.getLogicalType
    val serializer = new JsonRowDataSerializationSchema(
      rowType.asInstanceOf[RowType], TimestampFormat.SQL,JsonFormatOptions.MapNullKeyMode.FAIL, "null", false)

    rstDs.addSink(new LogSinkFunction[RowData]("print", serializer))
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
