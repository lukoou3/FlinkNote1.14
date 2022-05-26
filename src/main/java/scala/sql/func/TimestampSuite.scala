package scala.sql.func

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.connector.log.LogSinkFunction
import scala.sql.utils.TableImplicits._

class TimestampSuite extends AnyFunSuite with BeforeAndAfterAll{
  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    tEnv = StreamTableEnvironment.create(env, settings)

    val sql = """
    CREATE TABLE tmp_tb(
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
    ) WITH (
      'connector' = 'mylocalfile',
      'path' = 'D:\IdeaWorkspace\FlinkNote\Flink12\test_data\online_log.json',
      'source.sleep' = '1000',
      'format' = 'json',
      -- format的参数配置，前面需要加format的名称
      'json.fail-on-missing-field' = 'false',
      -- json解析报错会直接返回null(row是null), 没法跳过忽略, {}不会报错, 属性都是null
      'json.ignore-parse-errors' = 'true'
    )
    """
    tEnv.executeSql(sql)
  }

  test("current_date/current_timestamp"){
    val sql = """
    select
        current_date date1,
        current_timestamp timestamp1,
        localTimestamp localTimestamp1,
        -- 都不能转
        -- cast(current_timestamp as bigint) timestamp2,
        -- cast(localTimestamp as bigint) localTimestamp2,
        dt
    from tmp_tb
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    /**
     * TIMESTAMP_LTZ(3)
     * [[org.apache.flink.table.data.TimestampData]]
     *
     */
    rstTable.toAppendStream[RowData].addSink{ data =>
      println(data)
    }

    /**
     * 新的api:Row
     * TIMESTAMP_LTZ(3)
     * java.time.Instant
     * [[org.apache.flink.table.runtime.operators.sink.OutputConversionOperator]]
     * TIMESTAMP(3)
     * LocalDateTime
     */
    rstTable.toDataStream.addSink{ data =>
      println(data)
    }

    /**
     * TIMESTAMP_LTZ(3)
     * java.time.Instant
     * [[org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter]]
     */
    //rstTable.toAppendStream[Row].addSink{ data =>  println(data) }

    /**
     * [[org.apache.flink.table.utils.PrintUtils#formatTimestampField]]
     * timestampToString(instant.atZone(sessionTimeZone).toLocalDateTime(), getPrecision(fieldType))
     */
    // rstTable.execute().print()

    rstTable.toAppendStream[RowData].addSink(
      // new LogSinkFunction("warn", new SerializationSchemaLogWrapper(rstTable.getJsonRowDataSerializationSchema))
      new LogSinkFunction("warn", rstTable.getJsonRowDataSerializationSchema.wrapLog)
    )


  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
