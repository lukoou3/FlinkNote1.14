package scala.connector.test

import java.nio.charset.StandardCharsets

import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.json.{JsonFormatOptions, JsonRowDataSerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConverters._

object RowDataLogSinkFunctionTest {

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
    val rstDs = rstTable.toRetractStream[RowData].map(_._2)

    val funcs = rstTable.getResolvedSchema.getColumns.asScala.zipWithIndex.map{ case (col, i) =>
      val func:RowData => Any  = col.getDataType.getLogicalType.getTypeRoot match {
        case CHAR | VARCHAR => row => if(row.isNullAt(i)) null else row.getString(i).toString
        case INTEGER => row => if(row.isNullAt(i)) null else row.getInt(i)
        case BIGINT => row => if(row.isNullAt(i)) null else row.getLong(i)
        case FLOAT => row => if(row.isNullAt(i)) null else row.getFloat(i)
        case DOUBLE => row => if(row.isNullAt(i)) null else row.getDouble(i)
      }
      func
    }

    val rowType = rstTable.getResolvedSchema.toPhysicalRowDataType.getLogicalType
    val rowDataTypeInfo = rstDs.dataType
    val serializer = new JsonRowDataSerializationSchema(
      rowType.asInstanceOf[RowType], TimestampFormat.SQL,JsonFormatOptions.MapNullKeyMode.FAIL, "null", false)
    // BinaryRowData, GenericRowData
    rstDs.rebalance.addSink(row => {
      println(row)
      println(new String(serializer.serialize(row), StandardCharsets.UTF_8))
      println(funcs.map(x => x(row)))
    })

    //val dataType: TypeInformation[Row] = rstDs.dataType
    //val serializer = JsonRowSerializationSchema.builder().withTypeInfo(dataType).build()

    //rstDs.addSink(new LogSinkFunction[Row]("error", serializer))

    println("*" * 50)

    // 阻塞
    env.execute("SocketDynamicTableTest")
    println("end:" + "*" * 50)
  }

}
