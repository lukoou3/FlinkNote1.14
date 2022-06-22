package scala.connector.test

import org.apache.flink.client.deployment.executors.RemoteExecutor
import org.apache.flink.client.program.MiniClusterClient
import org.apache.flink.configuration.{ConfigConstants, Configuration, DeploymentOptions, JobManagerOptions, RestOptions, TaskManagerOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

object TestCluster {

  def main(args: Array[String]): Unit = {
    val config = new Configuration()
    config.set(TaskManagerOptions.NUM_TASK_SLOTS, 2:Integer)
    config.setInteger(JobManagerOptions.PORT, 0)

    val cluster = createLocalCluster(config)
    val port = cluster.getRestAddress.get.getPort

    setJobManagerInfoToConfig(config, "localhost", port)
    config.set(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    config.setBoolean(DeploymentOptions.ATTACHED, true)

    println(s"\nStarting local Flink cluster (host: localhost, port: ${port}).\n")

    val clusterClient = new MiniClusterClient(config, cluster)


    val remoteSenv = new org.apache.flink.streaming.api.environment.StreamExecutionEnvironment(config)
    val env = new StreamExecutionEnvironment(remoteSenv)
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    CREATE TABLE tmp_tb2 (
      name string,
      age int,
      cnt bigint,
      proctime TIMESTAMP_LTZ(3)
    ) WITH (
      'connector' = 'mylog',
      'log-level' = 'warn',
      'format' = 'json'
    )
    """
    tEnv.executeSql(sql)

    println("insert into before")
    tEnv.executeSql("insert into tmp_tb2 select * from tmp_tb1")
    println("insert into after")

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.toDataStream.addSink(println(_))

    env.execute("1111")
  }

  def main1(args: Array[String]): Unit = {
    val config = new Configuration()
    config.setInteger(JobManagerOptions.PORT, 0)

    val cluster = createLocalCluster(config)
    val port = cluster.getRestAddress.get.getPort

    setJobManagerInfoToConfig(config, "localhost", port)
    config.set(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    config.setBoolean(DeploymentOptions.ATTACHED, true)

    println(s"\nStarting local Flink cluster (host: localhost, port: ${port}).\n")

    val clusterClient = new MiniClusterClient(config, cluster)


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    Thread.sleep(1000 * 60 * 3)

    env.execute("")
  }

  private def createLocalCluster(flinkConfig: Configuration) = {

    val numTaskManagers = flinkConfig.getInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)
    val numSlotsPerTaskManager = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS)

    val miniClusterConfig = new MiniClusterConfiguration.Builder()
      .setConfiguration(flinkConfig)
      .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
      .setNumTaskManagers(numTaskManagers)
      .build()

    val cluster = new MiniCluster(miniClusterConfig)
    cluster.start()
    cluster
  }

  private def setJobManagerInfoToConfig(
    config: Configuration,
    host: String, port: Integer): Unit = {

    config.setString(JobManagerOptions.ADDRESS, host)
    config.setInteger(JobManagerOptions.PORT, port)

    config.setString(RestOptions.ADDRESS, host)
    config.setInteger(RestOptions.PORT, port)
  }
}
