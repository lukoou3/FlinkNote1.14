package scala.sql.types

import java.time.Instant

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Schema}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import SqlDateTypesSuite._

class SqlDateTypesSuite extends AnyFunSuite with BeforeAndAfterAll {
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

  /**
   * 之前的直接通过传入可变参数重命名列名和定义处理/事件时间的方法被标记废弃了
   * 这些方法都被标记废弃了，之后的版本可能会删除：fromDataStream(stream, $"myLong", $"myString")/toAppendStream/toRetractStream
   * 新版本使用这几个方法：fromDataStream/toDataStream, fromChangelogStream/toChangelogStream
   * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/data_stream_api/#examples-for-fromdatastream
   * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/data_stream_api/#legacy-conversion
   */
  test("ds转table") {
    val datas = Seq(
      People(1, "aaa", 20, 90.2, Instant.ofEpochMilli(1000)),
      People(2, "bb", 22, 90.2, Instant.ofEpochMilli(1001)),
      People(3, "cc", 21, 90.2, Instant.ofEpochMilli(1002))
    )

    val ds = env.fromCollection(datas)

    /**
     * 自动推断所有的physical columns
     * ds的.TypeInfo和sql的DataType的对应关系在：[[org.apache.flink.table.types.utils.TypeInfoDataTypeConverter.conversionMap]]
     * Instant对应TIMESTAMP_LTZ
     * sql内部的类型对应关系可以看：LogicalTypeUtils.toInternalConversionClass
     * sql类型的实际转化例子可以看官方实现的connector的读写，可以json转化的实现为例查看：JsonToRowDataConverters和RowDataToJsonConverters
     */
    var table = tEnv.fromDataStream(ds)
    table.printSchema()

    /**
     * 自动推断所有的physical columns
     * 同时添加计算列
     */
    table = tEnv.fromDataStream(ds, Schema.newBuilder()
      .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
      .columnByExpression("proc_time", "PROCTIME()")
      .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
      .build()
    )
    table.printSchema()

    table.execute().print()
  }

  test("table转ds"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    /**
     * 就是直接调用的： [[StreamTableEnvironment.toDataStream(Table)]]
     * 实际实现功能的是：[[org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl.toDataStream(Table)]]
     * 然后调用的代码是：
     *      // include all columns of the query (incl. metadata and computed columns)
     *      val sourceType = table.getResolvedSchema.toSourceRowDataType
     *      toDataStream(table, sourceType)
     * 没有bridged到RowData，所以生成的就是Row类型
     */
    val rowDs: DataStream[Row] = table.toDataStream
    println(rowDs.dataType)


    /**
     * 转RowData，用新的方法需要显示绑定使用内部类型
     * 使用toAppendStream[RowData]老的api则可以直接转为RowData。
     */
    val rowDataDataType: DataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs: DataStream[RowData] = table.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    println(rowDataDs.dataType)

    rowDataDs.addSink{row =>
      println(row)
    }
  }

  test("table转ds2"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
       -- 'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select name,age,cnt,data,datas from tmp_tb1")

    /**
     * table转ds会加一个filter校验过滤非空属性，默认data中的非空属性table这行实际的列值为null时会抛出异常
     * 设置'table.exec.sink.not-null-enforcer'='drop'后，会直接把这一行给直接过滤掉，肯定不能设置这个
     * 这个是运行时报错的，要是实际就没null，转的data中使用原生类型也是不会报错的。不是编译阶段报错
     * org.apache.flink.table.runtime.operators.sink.SinkNotNullEnforcer
     */
    val dataDs = table.toDataStream(classOf[TableCaseData])
    println(dataDs.dataType)
    //dataDs.print()
    dataDs.addSink{row =>
      println(row)
    }

  }

  test("table_execute_print"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
       -- 'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    /**
     * 实现的格式化方法在：org.apache.flink.table.utils.PrintUtils#rowToString()
     * 主要是格式化显示时间列
     */
    table.execute().print()

  }

  test("timestamp_convert"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      datetime timestamp(3),
      datetime_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.datetime.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.datetime_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    val rowDs = table.toDataStream
    //table.execute().print()
    /**
     * timestamp: LocalDateTime
     * datetime_ltz: Instant
     * 转化的operator是OutputConversionOperator，实际转化的类是RowRowConverter
     * 创建RowRowConverter的逻辑：[[org.apache.flink.table.data.conversion.RowRowConverter.create]]
     * 各个属性的转化：[[org.apache.flink.table.data.conversion.DataStructureConverters.getConverter]]
     * 各个属性转化的map: [[org.apache.flink.table.data.conversion.DataStructureConverters.converters]]
     * timestamp的转化：
     *    LocalDateTime：直接把时间戳传进去创建，LocalDateTime这玩意里面的时间戳似乎是没时区感知的
     *    Timestamp：使用TimestampTimestampConverter，Timestamp.valueOf(toLocalDateTime())，直接把LocalDateTime的year,month...传进去了，相当于utc的时间
     *
     * datetime_ltz的转化：
     *    Instant：相当于直接返回时间戳
     *    Timestamp：使用LocalZonedTimestampTimestampConverter，直接把时间戳传进去了，有时区感知
     *    Long：直接返回时间戳
     */
    rowDs.addSink{ row =>
      println(row)
    }

  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}

object SqlDateTypesSuite {

  case class People(id: Long, name: String, age: Int, score: Double, event_time: Instant )
  case class TableCaseSubData(name: String, age: Integer)
  //case class TableCaseData(name: String, age: Integer, cnt: java.lang.Long, data: TableCaseSubData, datas:java.util.List[TableCaseSubData])
  //case class TableCaseData(name: String, age: Integer, cnt: java.lang.Long, data: TableCaseSubData, datas:Array[TableCaseSubData])

  case class TableCaseData(name: String, age: Int, cnt: java.lang.Long, data: TableCaseSubData, datas:Array[TableCaseSubData])

}
