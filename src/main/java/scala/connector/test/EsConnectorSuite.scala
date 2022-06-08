package scala.connector.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.connector.es._

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

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
