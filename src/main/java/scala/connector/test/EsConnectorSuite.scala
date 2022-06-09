package scala.connector.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.connector.es._
import EsConnectorSuite._

class EsConnectorSuite extends AnyFunSuite with BeforeAndAfterAll {
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

  test("insert") {
    var sql =
      """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    select
        id _id,
        id title,
        name author,
        cast(id as int) `year`,
        concat(id, '_', name) content
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    sql =
      """
    CREATE TABLE tmp_tb2 (
      `_id` STRING,
      `title` STRING,
      `author` STRING,
      `year` INT,
      `content` STRING
    ) WITH (
      'connector' = 'myes',
      'cluster-name' = 'localhost',
      'resource' = 'index_test2/type_test',
      'es.index.auto.create' = 'true',
      'es.mapping.id' = '_id'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    insert into tmp_tb2
    select
        id _id,
        id title,
        name author,
        cast(id as int) `year`,
        concat(id, '_', name) content
    from tmp_tb1
    """
    tEnv.executeSql(sql)

    Thread.sleep(1000 * 60 * 60)
    //rstTable.execute().print()
  }

  test("addRowDataBatchIntervalEsSink") {
    var sql =
      """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    select
        id _id,
        id title,
        name author,
        cast(id as int) `year`,
        concat(id, '_', name) content
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.addRowDataBatchIntervalEsSink(Map(
      ES_RESOURCE_WRITE -> "index_test3/type_test",
      ES_INDEX_AUTO_CREATE -> "true",
      ES_MAPPING_ID -> "_id"
    ), 10, 5000)

  }

  test("addRowDataBatchIntervalEsSink复杂类型"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      id int,
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
       -- 'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select id,name,age,cnt,data,datas from tmp_tb1")

    table.addRowDataBatchIntervalEsSink(Map(
      ES_RESOURCE_WRITE -> "index_test5/type_test",
      ES_INDEX_AUTO_CREATE -> "true",
      ES_MAPPING_ID -> "id"
    ), 10, 5000)
  }

  test("addBatchIntervalEsSink"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      id int,
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
       -- 'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select id,name,age,cnt,data,datas from tmp_tb1")

    val dataDs = table.toDataStream(classOf[TableCaseData])

    // 通过运行时反射获取scala/java字段信息
    dataDs.addBatchIntervalEsSink(Map(
      ES_RESOURCE_WRITE -> "index_test4/type_test",
      ES_INDEX_AUTO_CREATE -> "true",
      ES_MAPPING_ID -> "id"
    ), 10, 5000)
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}

object EsConnectorSuite{
  case class TableCaseSubData(name: String, age: Integer)
  case class TableCaseData(id:Int, name: String, age: Int, cnt: java.lang.Long, data: TableCaseSubData, datas:Array[TableCaseSubData])
}