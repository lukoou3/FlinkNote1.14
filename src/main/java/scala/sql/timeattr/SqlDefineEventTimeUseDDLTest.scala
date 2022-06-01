package scala.sql.timeattr

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * 和1.12一样。
 * 我靠：这个窗口时间的显示rstTable.execute().print()和rstTable.toAppendStream[Row]时间的显示不一样
 * table的tostring就是当前的时区时间，toAppendStream[Row]后和1.12一样不是当前的时区，之后看看区别
 *
 * sql中定义事件时间的三种方式: https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/concepts/time_attributes/#event-time
 * 定义事件时间一般也就只会用到两种, 直接在最后一列额外声明一个处理时间字段即可, 形式是固定的:
 *    在创建表的 DDL 中定义, 格式如像: WATERMARK FOR ts AS ts - INTERVAL '5' SECOND, 指定存在的列为事件时间并生成延时5s的水位线
 *    在 DataStream 到 Table 转换时定义, 格式如像: 'eventTime.rowtime() as 'event_time, Watermark需要在DataStream中定义好
 */
object SqlDefineEventTimeUseDDLTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.port", "8082")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)


    // 每个分区：每秒1个。整个应用：每秒2个，5秒10个。使用事件时间, 实际1s, 事件时间2s, 5秒5个。
    // datagen的sequence是全局递增的, 两个分区每秒2个, 事件时间2s。其实不用想那么多, 一个元素1秒, 5秒也就是5个元素。
    val createTbSql =
    """
CREATE TABLE tmp_tb (
  page_id int,
  cnt int,
  time_long bigint,
  time_str as FROM_UNIXTIME(time_long,'yyyy-MM-dd HH:mm:ss'),
  ts as TO_TIMESTAMP(FROM_UNIXTIME(time_long,'yyyy-MM-dd HH:mm:ss')),
  proctime as PROCTIME(),
  -- 声明 ts 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'='datagen',
  'rows-per-second'='1',
  'fields.page_id.min'='0',
  'fields.page_id.max'='0',
  'fields.cnt.min'='1',
  'fields.cnt.max'='1',
  'fields.time_long.kind'='sequence',
  'fields.time_long.start'='1636300800', -- new Date("2021-11-08 00:00:00").getTime()/1000;
  'fields.time_long.end'='1636387200' -- new Date("2021-11-08 00:00:00").getTime()/1000 + 60 * 60 * 24;
)
    """
    tEnv.executeSql(createTbSql)

    //tEnv.sqlQuery("select * from tmp_tb").execute().print()

    var sql =
      """
    select
        TUMBLE_START(ts, INTERVAL '5' SECOND) as wstart,
        TUMBLE_END(ts, INTERVAL '5' SECOND) as wend,
        min(time_str) min_time,
        max(time_str) max_time,
        page_id,
        sum(cnt) pv,
        count(1) pv2
    from tmp_tb
    group by page_id, TUMBLE(ts, INTERVAL '5' SECOND)
    """

    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    /**
     * 这个查询竟然都不是insert-only的，我记得使用处理时间的滚动窗口聚合是insert-only，这里使用事件时间但是没有配置允许迟到，不知道他这怎么不是insert-only模式
     *
     * 这个聚合输出确实是insert模式，报错的原因是sink的table被显式的设置成1，报错。原因还不知道为啥。感觉没必须校验这个。
     *
     * The sink for table 'default_catalog.default_database.temp_tb' has a configured parallelism of 1, while the input parallelism is -1.
     *
     * Since the configured parallelism is different from the input's parallelism and the changelog mode is not insert-only, a primary key is required but could not be found.
     *
     * 之前看错了，sink支持insert和UPDATE_AFTER，并行度发生变化时才会报错，可可以理解，要支持更新得把主键发到对应的分区
     *
     * 显示配置这个这个算子的并行度为1时，上游没设置并行度，得设置ChangelogMode为insert-only，或者在输出表定义唯一key
     *
     * sql每个查询的输出模式都需要注意，这也是比较重要的，万一输出模式是有更新了，但是我们不想要更新，可以用一个只支持inset的skin验证下
     */
    sql = """
    create temporary table temp_tb (
        `wstart` TIMESTAMP(3) NOT NULL,
        `wend` TIMESTAMP(3) NOT NULL,
        `min_time` VARCHAR(2000),
        `max_time` VARCHAR(2000),
        `page_id` INT,
        `pv` INT,
        `pv2` BIGINT NOT NULL
    ) with (
      -- 'connector' = 'print'
      'connector' = 'mylocalfile',
      'path' = 'D:\temp_tb.csv',
      'format' = 'csv'
    )
    """
    tEnv.executeSql(sql)


    sql = """
    insert into temp_tb
    select
        TUMBLE_START(ts, INTERVAL '5' SECOND) as wstart,
        TUMBLE_END(ts, INTERVAL '5' SECOND) as wend,
        min(time_str) min_time,
        max(time_str) max_time,
        page_id,
        sum(cnt) pv,
        count(1) pv2
    from tmp_tb
    group by page_id, TUMBLE(ts, INTERVAL '5' SECOND)
    """
    tEnv.executeSql(sql)

    //rstTable.toAppendStream[Row].print()
    //tEnv.toRetractStream[Row](rstTable).print()
    //rstTable.execute().print()

    env.execute()
  }

}
