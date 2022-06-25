package scala.connector.test

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.json.{JsonFormatOptions, JsonRowDataSerializationSchema}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.connector.common.{BatchIntervalSink, Utils}

import BatchIntervalSinkKeyedModeOrderBySuite._

class BatchIntervalSinkKeyedModeOrderBySuite extends AnyFunSuite with BeforeAndAfterAll {
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

  test("1key_orderby_1colasc"){
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      age int,
      time_ltz timestamp_ltz(3),
      time_ts as unix_timestamp(cast(time_ltz as string))
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''5''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.addRowDataBatchIntervalJsonPrintSink(
      10,
      5000,
      keyedMode = true,
      keys = Seq("id"),
      orderBy = Seq(("time_ts", true))
    )
  }

  test("1key_orderby_1coldesc"){
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      age int,
      time_ltz timestamp_ltz(3),
      time_ts as unix_timestamp(cast(time_ltz as string))
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''5''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.addRowDataBatchIntervalJsonPrintSink(
      10,
      5000,
      keyedMode = true,
      keys = Seq("id"),
      orderBy = Seq(("time_ts", false))
    )
  }

  test("1key_orderby_1colasc_2colasc"){
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      age int,
      time_ltz timestamp_ltz(3),
      time_ts as unix_timestamp(cast(time_ltz as string))
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''2''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.time_ltz.expression' =  '#{date.past ''3'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.addRowDataBatchIntervalJsonPrintSink(
      10,
      5000,
      keyedMode = true,
      keys = Seq("id"),
      orderBy = Seq(("time_ts", true), ("age", true))
    )
  }

  test("1key_orderby_1colasc_2coldesc"){
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      age int,
      time_ltz timestamp_ltz(3),
      time_ts as unix_timestamp(cast(time_ltz as string))
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''2''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.time_ltz.expression' =  '#{date.past ''2'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.addRowDataBatchIntervalJsonPrintSink(
      10,
      5000,
      keyedMode = true,
      keys = Seq("id"),
      orderBy = Seq(("time_ts", true), ("age", false))
    )
  }

  test("2key_orderby_1colasc"){
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      age int,
      time_ltz timestamp_ltz(3),
      time_ts as unix_timestamp(cast(time_ltz as string))
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''2''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.addRowDataBatchIntervalJsonPrintSink(
      10,
      5000,
      keyedMode = true,
      keys = Seq("id", "name"),
      orderBy = Seq(("time_ts", true))
    )
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}

object BatchIntervalSinkKeyedModeOrderBySuite{

  implicit class TableFunctions(table: Table){
    def addRowDataBatchIntervalJsonPrintSink(
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      keyedMode: Boolean = false,
      keys: Seq[String] = Nil,
      orderBy: Seq[(String, Boolean)] = Nil
    ): DataStreamSink[RowData] = {
      val sink = getRowDataBatchIntervalJsonPrintSink(table.getResolvedSchema, batchSize, batchIntervalMs, minPauseBetweenFlushMs,
        keyedMode = keyedMode, keys=keys, orderBy=orderBy)
      val rowDataDs = table.toDataStream[RowData](table.getResolvedSchema.toSourceRowDataType.bridgedTo(classOf[RowData]))
      rowDataDs.addSink(sink)
    }
  }

  def getRowDataBatchIntervalJsonPrintSink(
    resolvedSchema: ResolvedSchema,
    batchSize: Int,
    batchIntervalMs: Long,
    minPauseBetweenFlushMs: Long = 100L,
    keyedMode: Boolean = false,
    keys: Seq[String] = Nil,
    orderBy: Seq[(String, Boolean)] = Nil
  ): BatchIntervalSink[RowData] = {
    val rowType = resolvedSchema.toPhysicalRowDataType.getLogicalType
    val typeInformation: InternalTypeInfo[RowData] = InternalTypeInfo.of(rowType)
    val jsonSerializer = new JsonRowDataSerializationSchema(
      rowType.asInstanceOf[RowType], TimestampFormat.SQL, JsonFormatOptions.MapNullKeyMode.FAIL, "null", true)
    val _getKey = Utils.getTableKeyFunction(resolvedSchema,keyedMode,keys,orderBy)
    val tableOrdering = Utils.getTableOrdering(resolvedSchema, orderBy)
    new BatchIntervalSink[RowData](batchSize, batchIntervalMs, minPauseBetweenFlushMs, keyedMode){
      @transient var serializer: TypeSerializer[RowData] = _
      @transient var objectReuse = false

      override def onInit(parameters: Configuration): Unit = {
        objectReuse = getRuntimeContext.getExecutionConfig.isObjectReuseEnabled
        if(objectReuse){
          serializer = typeInformation.createSerializer(getRuntimeContext.getExecutionConfig)
        }
      }

      override def valueTransform(data: RowData): RowData = {
        if(objectReuse) serializer.copy(data) else data
      }

      override def getKey(data: RowData): Any = _getKey(data)

      override def replaceValue(newValue: RowData, oldValue: RowData): RowData = if (!this.keyedMode) {
        super.replaceValue(newValue, oldValue)
      } else {
        if (tableOrdering.gteq(newValue, oldValue)) {
          println(s"""${new String(jsonSerializer.serialize(newValue), "utf-8")}\n>=\n${new String(jsonSerializer.serialize(oldValue), "utf-8")}""")
          newValue
        } else {
          println(s"""${new String(jsonSerializer.serialize(newValue), "utf-8")}\n<\n${new String(jsonSerializer.serialize(oldValue), "utf-8")}""")
          oldValue
        }
      }

      override def onFlush(datas: Iterable[RowData]): Unit = {
        for (data <- datas) {
          println("onFlush", new String(jsonSerializer.serialize(data), "utf-8"))
        }
      }

      override def onClose(): Unit = {}
    }
  }
}
