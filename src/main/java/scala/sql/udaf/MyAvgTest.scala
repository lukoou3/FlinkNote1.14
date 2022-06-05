package scala.sql.udaf

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

object MyAvgTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.createTemporarySystemFunction("myAvg", classOf[MyAvg])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.5',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        sum(age) sum_age,
        count(age) cnt_age,
        myAvg(age) avg_age
    from tmp_tb1
    group by name
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
    /*rstTable.toAppendStream[Row].addSink{row =>
      println(row)
    }*/

    env.execute("MyAvgTest")
  }

}
