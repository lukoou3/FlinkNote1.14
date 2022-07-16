package scala.file.sql

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 这个使用和flink1.12一样
 */
object FileSystemTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    //val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    env.enableCheckpointing(1000 * 30)
    env.setStateBackend(new HashMapStateBackend())
    // 又一个过时的方法
    //env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointStorage("file:///F:/flink-checkpoints")


    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val datas: DataStream[(String, Long, String)] = text.map { line =>
      val time = System.currentTimeMillis() + 60 * 60 * 1000
      (fmt.format(new Date(time)), time, line)
    }

    // 这个怎么过期了
    //tEnv.createTemporaryView("tmp_tb2", datas, 'dt_time, 'time_long, 'content)
    tEnv.fromDataStream(datas,
      Schema.newBuilder()
      .columnByExpression("dt_time", "_1")
        .build()
    ).printSchema()
    val table = tEnv.fromDataStream(datas).renameColumns('_1 as "dt_time", '_2 as "time_long", '_3 as "content")
    table.printSchema()
    tEnv.createTemporaryView("tmp_tb", table)


    val createTbSql =
      """
CREATE TABLE fs_table (
  dt_time string,
  time_long bigint,
  content string,
  dt string,
  `hour` string
) PARTITIONED BY (dt, `hour`) WITH (
  'connector'='filesystem',
  'path'='file:///F:/flink-fileSink/filesystem',
  'format'='orc',
  'sink.partition-commit.trigger'='process-time',
  'sink.partition-commit.delay'='0 s',
  'sink.partition-commit.policy.kind'='success-file',
  -- 'sink.partition-commit.policy.kind'='metastore',
  'sink.rolling-policy.file-size'='1MB',
  'sink.rolling-policy.rollover-interval'='2 min',
  'sink.rolling-policy.check-interval'='1 min'
)
      """

    tEnv.executeSql(createTbSql)

    val sql =
      """
insert into fs_table
select
    dt_time,
    time_long,
    content,
    substr(dt_time, 1, 10) dt,
    substr(dt_time, 12, 2) `hour`
from tmp_tb
      """

    tEnv.executeSql(sql)


    env.execute("FileSystemTest")
  }

}
