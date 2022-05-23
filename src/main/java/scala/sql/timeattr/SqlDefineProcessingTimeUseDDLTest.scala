package scala.sql.timeattr

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 * 和1.12一样，就是创建表时传入Expression*的方法已弃用，还有就是窗口时间toString显示的是当期时区的时间
 * DataStream-to-Table Conversion时定义proctime官网还是使用的弃用的方法fromDataStream[T](dataStream: DataStream[T], fields: Expression*)
 * 既然标记成弃用，官网还是用之前的代码真是蛋疼，而且中文的很多页面都是之前版本的页面，既然还没翻译直接引用英文的不行吗，真是服了flink
 *
 * sql中定义处理时间的三种方式: https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/concepts/time_attributes/#processing-time
 * 定义处理时间一般也就只会用到两种, 直接在最后一列额外声明一个处理时间字段即可, 形式是固定的:
 *    在创建表的 DDL 中定义, 格式如像: ts AS PROCTIME()
 *    在 DataStream 到 Table 转换时定义, 格式如像: 'ts.proctime
 */
object SqlDefineProcessingTimeUseDDLTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.port", "8082")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 每个分区：每秒1个。整个应用：每秒2个，5秒10个
    val createTbSql =
    """
CREATE TABLE tmp_tb (
  page_id int,
  cnt int,
  proc_time AS PROCTIME() -- 声明一个额外的列作为处理时间属性
) WITH (
  'connector'='datagen',
  'rows-per-second'='1',
  'fields.page_id.min'='0',
  'fields.page_id.max'='0',
  'fields.cnt.min'='1',
  'fields.cnt.max'='1'
)
    """
    tEnv.executeSql(createTbSql)

    val sql =
    """
    select
        TUMBLE_START(proc_time, INTERVAL '5' SECOND) as wstart,
        TUMBLE_END(proc_time, INTERVAL '5' SECOND) as wend,
        min(proc_time) min_proc_time,
        max(proc_time) max_proc_time,
        page_id,
        sum(cnt) pv
    from tmp_tb
    group by page_id, TUMBLE(proc_time, INTERVAL '5' SECOND)
    """

    val rstTable = tEnv.sqlQuery(sql)

    //rstTable.toAppendStream[Row].print()
    rstTable.execute().print()

    env.execute()
  }

}
