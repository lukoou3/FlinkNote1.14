package scala.sql.udaf.internal

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class InternalAggFuncSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  /**
   * 内置的agg函数没问题，内部做了处理。他娘的，函数序列化的问题到1.14了还是这样。
   */
  test("collect"){
    var sql = """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      level int,
      score double,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{regexify ''(1|2){1}''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.name.null-rate' = '0.2',
      'fields.level.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.score.expression' = '#{number.numberBetween ''60'',''100''}',
      'rows-per-second' = '5'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id,
        collect(name) names,
        collect(level) levels
    from tmp_tb1
    group by id
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  /**
   * 这个agg函数的序列化后竟然只有一个，每个注册的name只会是最后一个，太坑了，修改对象的属性也不行
   * 可以通过实现SpecializedFunction接口实现运行时new一个function，这样udf就能区分开了
   * agg函数中不能获取字面量的参数，不论是是否实现SpecializedFunction，flink真是行，udf就没事
   */
  test("value_sum"){
    tEnv.createTemporarySystemFunction("value_sum", classOf[ValueSumAggFunction])
    var sql = """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      level int,
      score double,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{regexify ''(1|2){1}''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.name.null-rate' = '0.2',
      'fields.level.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.score.expression' = '#{number.numberBetween ''60'',''100''}',
      'rows-per-second' = '5'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id,
        value_sum(name, level) agg1,
        value_sum(name, score) agg2
        -- value_sum1(name, level) agg1,
        -- value_sum2(name, score) agg2
    from tmp_tb1
    group by id
    """
    val rstTable = tEnv.sqlQuery(sql)
    //rstTable.printSchema()
    //println(rstTable.explain())

    rstTable.execute().print()
  }

  test("value_count"){
    tEnv.createTemporarySystemFunction("value_count", classOf[ValueCountAggFunction])
    var sql = """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      level int,
      score double,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{regexify ''(1|2){1}''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.name.null-rate' = '0.2',
      'fields.level.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.score.expression' = '#{number.numberBetween ''60'',''100''}',
      'rows-per-second' = '5'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id,
        value_count(name) names1,
        value_count(name, 1) names2,
        value_count(level) level1,
        value_count(level, 1) level2
    from tmp_tb1
    group by id
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    //println(rstTable.explain())

    rstTable.execute().print()
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
