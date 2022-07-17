package scala.sql.types

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.data.RowData
import org.apache.flink.types.{Row, RowKind}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.sql.utils.TableImplicits._
class ChangelogRowDataStreamSuite extends AnyFunSuite with BeforeAndAfterAll {
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

  test("toChangelogStream_Row") {
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
        name,
        count(age) age
    from tmp_tb1 group by name
    """
    val table = tEnv.sqlQuery(sql)

    // api显示返回的就只是Row类型
    val rowDs: DataStream[Row] = table.toChangelogStream
    println(rowDs.dataType)

    rowDs.addSink{row =>
      println(row.getKind, row)
    }

  }

  test("toChangelogStream_RowData") {
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
        name,
        count(age) age
    from tmp_tb1 group by name
    """
    val table = tEnv.sqlQuery(sql)

    val rowDataDs: DataStream[RowData] = scala.connector.common.Utils.getRowDataDataStreamInternal(table, null)
    println(rowDataDs.dataType)

    val serializer = table.getJsonRowDataSerializationSchema
    rowDataDs.addSink{row =>
      println(row.getRowKind.shortString(), new String(serializer.serialize(row), StandardCharsets.UTF_8))
    }

  }

  test("toChangelogStream_RowData_UPDATE_AFTER") {
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
        name,
        count(age) age
    from tmp_tb1 group by name
    """
    val table = tEnv.sqlQuery(sql)

    val changelogMode = ChangelogMode.newBuilder
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .build

    /**
     * GroupAggFunction有个属性generateUpdateBefore判断是否输出UPDATE_BEFORE messages.
     * [[org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction#processElement()]]
     */
    val rowDataDs: DataStream[RowData] = scala.connector.common.Utils.getRowDataDataStreamInternal(table, changelogMode)
    println(rowDataDs.dataType)

    val serializer = table.getJsonRowDataSerializationSchema
    rowDataDs.addSink{row =>
      println(row.getRowKind.shortString(), new String(serializer.serialize(row), StandardCharsets.UTF_8))
    }

  }

  /**
   * 编译就会报错，不能不消费update消息
   * 看下org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction
   * 里面有DELETE,INSERT,UPDATE_BEFORE,UPDATE_AFTER消息的生成
   * 一般也不会有DELETE消息,除非消息来源中有DELETE消息吧，似乎还没看到生成DELETE消息的
   *    在deduplication消息中做聚合就会生成DELETE消息
   * 这种DELETE消息最好不要处理，感觉怪怪的，而且也和状态有关
   */
  test("toChangelogStream_RowData_INSERT_ONLY") {
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
        name,
        count(age) age
    from tmp_tb1 group by name
    """
    val table = tEnv.sqlQuery(sql)

    val changelogMode = ChangelogMode.insertOnly()

    /**
     * GroupAggFunction有个属性generateUpdateBefore判断是否输出UPDATE_BEFORE messages.
     * [[org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction#processElement()]]
     */
    val rowDataDs: DataStream[RowData] = scala.connector.common.Utils.getRowDataDataStreamInternal(table, changelogMode)
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
