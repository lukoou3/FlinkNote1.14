package scala.sql.udf

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * 通过查看[[org.apache.flink.table.planner.codegen.OperatorCodeGenerator#generateOneInputStreamOperator]]
 * 可以看到Nvl对string列的处理，Nvl会有两次类型转换的操作，而NvlInternal就没有类型转换的操作，这在有些时候性能会有提升
 */
object NvlInternalFuncTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 两种注册的方式
    tEnv.createTemporarySystemFunction("nvl1", classOf[Nvl])
    tEnv.createTemporarySystemFunction("nvl2", new NvlInternal)
    tEnv.createTemporarySystemFunction("nvl3", new NvlInternal2)
    tEnv.createTemporarySystemFunction("nvl4", new NvlInternal3)

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      code int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.5',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.code.null-rate' = '0.5',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.5',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        nvl1(name, cast('1' as string)) name1,
        nvl2(name, cast('2' as string)) name2,
        nvl3(name, '2') name3,
        -- 不能强转类型转换
        -- nvl3(name, 6) name4,
        nvl4(name, '2') name4,
        nvl4(code, cnt) code1,
        nvl4(cnt, 34) cnt1,
        '2' c,
        code,
        cnt,
        proctime
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
    /*rstTable.toAppendStream[Row].addSink{row =>
      println(row)
    }*/

    env.execute("NvlInternalFuncTest")
  }

  def main2(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)


    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      code int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.5',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.code.null-rate' = '0.5',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.5',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        code,
        cnt,
        coalesce(name, '2', 3) name1,
        coalesce(code, 1) code1,
        coalesce(cnt, 1) cnt1,
        proctime
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
    /*rstTable.toAppendStream[Row].addSink{row =>
      println(row)
    }*/

    env.execute("NvlInternalFuncTest")
  }

}
