package scala.sql.udf.internal

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class InternalFuncSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  test("nvl"){
    tEnv.createTemporarySystemFunction("nvl", classOf[Nvl])
    tEnv.createTemporarySystemFunction("named_struct", classOf[CreateNamedStruct])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      code int,
      cnt bigint,
      data row<name string, age int>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.3',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.code.null-rate' = '0.3',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.3',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        code,
        cnt,
        -- 可以提示类型转换
        nvl(name, '2') name1,
        nvl(code, 111) code1,
        nvl(code, cnt) code2,
        nvl(cnt, 66666666666) cnt1,
        nvl(cnt, code) cnt2,
        data,
        nvl(data, null) data1,
        nvl(data, named_struct('name', name, 'age', age)) data2,
        -- 对应的类型相同FieldsDataType的equals返回true
        nvl(data, named_struct('name1', name, 'age1', age)) data3,
        proctime
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("substring_index"){
    tEnv.createTemporarySystemFunction("substring_index", classOf[SubstringIndex])

    var sql = """
    CREATE TABLE tmp_tb1 (
      time_str string,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.time_str.expression' = '#{date.past ''15'',''SECONDS''}',
      'fields.name.null-rate' = '0.3',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        time_str,
        substring_index(time_str, '-', 1) str1,
        substring_index(time_str, '-', 2) str2,
        substring_index(time_str, '-', 3) str3,
        substring_index(time_str, ':', -1) str_1,
        substring_index(time_str, ':', -2) str_2,
        substring_index(time_str, ' ', -1) str_3,
        proctime
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
