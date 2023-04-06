package scala.connector.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.types.logical.RowType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._


class TableSchemaSqlSchemaConvertSuite extends AnyFunSuite with BeforeAndAfterAll {
  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    conf.setString("rest.bind-port", "8081-8085")
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    tEnv = StreamTableEnvironment.create(env, settings)
  }

  test("sql2DataType"){
    val dataType = tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
      .createDataType("ROW<name string, age int, cnt bigint>")
    println(dataType.toString)
    println(dataType.toString.toLowerCase)
    println(dataType.getLogicalType.toString.toLowerCase)
    println(dataType.getLogicalType.asInstanceOf[RowType].getFields.asScala.map(_.asSummaryString).mkString("ROW<", ", ", ">"))
    println(dataType.getLogicalType.asInstanceOf[RowType].getFields.asScala.map(_.asSummaryString).mkString(",\n"))
  }

  test("DataType2sql"){
    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      names array<string>,
      data row<name string, age int>,
      datas array<row<name string, age int>>,
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
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        *
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println("*" * 20)
    println(rstTable.getResolvedSchema.toString.toLowerCase)
    println("*" * 20)
    rstTable.getResolvedSchema.getColumns.asScala.map{col =>
      val name = col.getName
      val dataType = col.getDataType
      println(s"$name ${dataType.toString.toLowerCase}")
    }
  }


  override protected def afterAll(): Unit = {
    //env.execute()
  }
}
