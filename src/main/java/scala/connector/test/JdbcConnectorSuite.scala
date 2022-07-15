package scala.connector.test

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import scala.connector.jdbc._
class JdbcConnectorSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  test("addRowDataBatchIntervalJdbcSink") {
    var sql =
      """
    CREATE TABLE tmp_tb1 (
      code int,
      name string,
      age int,
      birthday string
    ) WITH (
      'connector' = 'faker',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''18'',''22''}',
      'fields.birthday.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    select
        *
    from tmp_tb1
    """
    val table = tEnv.sqlQuery(sql)

    table.addRowDataBatchIntervalJdbcSink(JdbcSinkParams(
      "people",
      JdbcConnectionOptions("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8", "root",
        "123456", "com.mysql.jdbc.Driver"),
      5, 3000
    ))
  }

  test("addBatchIntervalJdbcSink") {
    env.getConfig.disableObjectReuse()
    var sql =
      """
    CREATE TABLE tmp_tb1 (
      code int,
      name string,
      age int,
      birthday string
    ) WITH (
      'connector' = 'faker',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''18'',''22''}',
      'fields.birthday.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    select
        *
    from tmp_tb1
    """
    val table = tEnv.sqlQuery(sql)
    import JdbcConnectorSuite._
    table.toDataStream[People](classOf[People]).addBatchIntervalJdbcSink(
      "people",
      JdbcConnectionOptions("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8", "root",
        "123456", "com.mysql.jdbc.Driver"),
      5, 3000
    )
  }

  test("addKeyedBatchIntervalJdbcSink") {
    env.getConfig.disableObjectReuse()
    var sql =
      """
    CREATE TABLE tmp_tb1 (
      code int,
      name string,
      age int,
      birthday string
    ) WITH (
      'connector' = 'faker',
      'fields.code.expression' = '#{number.numberBetween ''0'',''2''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''18'',''22''}',
      'fields.birthday.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '3'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    select
        *
    from tmp_tb1
    """
    val table = tEnv.sqlQuery(sql)
    import JdbcConnectorSuite._
    table.toDataStream[People](classOf[People]).addKeyedBatchIntervalJdbcSink(_.code)(identity)(
      "people",
      JdbcConnectionOptions("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8", "root",
        "123456", "com.mysql.jdbc.Driver"),
      5, 5000
    )
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}

object JdbcConnectorSuite{
  case class People(
    code: Int,
    name: String,
    age: Int,
    birthday: String
  )
}