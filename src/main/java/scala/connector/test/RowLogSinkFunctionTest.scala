package scala.connector.test

import java.nio.charset.StandardCharsets
import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.csv.CsvRowSerializationSchema
import org.apache.flink.formats.json.{JsonFormatOptions, JsonRowDataDeserializationSchema, JsonRowDataSerializationSchema, JsonRowSerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.types.logical.{IntType, RowType}
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, CHAR, DOUBLE, FLOAT, INTEGER, VARCHAR}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.serialization.SerializationSchemaLogWrapper
import scala.connector.log.LogSinkFunction

object RowLogSinkFunctionTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    // Flink 1.14后，旧的planner被移除了，默认就是BlinkPlanner
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    /**
     * {"id":"1","name":"罗隐32","age":1300}
     * {"id":"1", "name":"罗隐", "age":30}
     * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
     */
    var sql = """
    CREATE TABLE tmp_tb1 (
      id int,
      name string,
      age int
      -- PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
      'connector' = 'mysocket',
      'hostname' = 'localhost',
      'port' = '9999',
      'format' = 'json',
      -- format的参数配置，前面需要加format的名称
      'json.fail-on-missing-field' = 'false',
      -- json解析报错会直接返回null(row是null), 没法跳过忽略, {}不会报错, 属性都是null
      'json.ignore-parse-errors' = 'true'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id,
        name,
        age,
        count(1) cnt
    from tmp_tb1
    group by id, name, age
    """
    val rstTable = tEnv.sqlQuery(sql)
    //rstTable.getResolvedSchema.
    //val rstDs = rstTable.toAppendStream[RowData]
    val rstDs = rstTable.toRetractStream[Row].filter(_._1).map(_._2)

    // GenericType<org.apache.flink.types.Row>
    //val dataType: TypeInformation[Row] = rstDs.dataType
    //println(dataType)
    val dataType = TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(rstTable.getResolvedSchema.toPhysicalRowDataType).asInstanceOf[TypeInformation[Row]]
    println(dataType)
    val serializer = JsonRowSerializationSchema.builder().withTypeInfo(dataType).build()
    val csvSerializer = new CsvRowSerializationSchema.Builder(dataType).build()

    //rstDs.addSink(new LogSinkFunction[Row]("error", serializer))
    //rstDs.addSink(new LogSinkFunction[Row]("error", csvSerializer))
    rstDs.addSink(new LogSinkFunction[Row]("error", new SerializationSchemaLogWrapper(serializer)))

    // 阻塞
    env.execute("SocketDynamicTableTest")
    println("end:" + "*" * 50)
  }

}
