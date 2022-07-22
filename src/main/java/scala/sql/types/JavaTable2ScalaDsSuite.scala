package scala.sql.types

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.sql.utils.TableImplicits.TableOps

class JavaTable2ScalaDsSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  test("a"){
    val sql = """
create temporary table server_logs (
    client_ip string,
    client_identity string,
    userid string,
    user_agent string,
    request_line string,
    status_code string,
    size int
) with (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgentAny}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}',
  'number-of-rows' = '20',
  'sleep-per-row' = '500'
)
"""
    tEnv.executeSql(sql)
    println("*" * 50)

    val table = tEnv.sqlQuery("select * from server_logs")
    println("*" * 50)
    //val ds = new TableOps(table).toJsonDataStream()
    //ds.print()
    table.execute().print()
    println("*" * 50)
  }


  override protected def afterAll(): Unit = {
    env.execute()
  }
}
