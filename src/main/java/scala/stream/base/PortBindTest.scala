package scala.stream.base

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}
import org.apache.flink.streaming.api.scala._

/**
 * 通过配置rest.bind-port，可以支持端口被占用时使用其它端口尝试
 * rest.bind-port: 8081-8085
 * rest.bind-port: 8081,8082,8083
 */
object PortBindTest {

  def main3(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.bind-port", "8081-8085")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val text: DataStream[String] = env.fromElements("1", "2", "3")
    text.map{ele =>
      Thread.sleep(1000 * 60 * 5)
      ele
    }.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.bind-port", "8081-8085")
    val miniClusterConfig = new MiniClusterConfiguration.Builder()
      .setConfiguration(conf)
      .setNumSlotsPerTaskManager(1)
      .setNumTaskManagers(1)
      .build()

    val cluster = new MiniCluster(miniClusterConfig)
    cluster.start()

    val port = cluster.getRestAddress.get.getPort
    println(cluster.getRestAddress.get)
    println(port)

    Thread.sleep(1000 * 60 * 5)
  }

  def main1(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val text: DataStream[String] = env.fromElements("1", "2", "3")
    text.map{ele =>
      Thread.sleep(1000 * 60 * 5)
      ele
    }.print()

    env.execute()
  }

}
