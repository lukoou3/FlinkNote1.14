package scala.sql.types

import java.time.Instant

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.client.deployment.executors.RemoteExecutor
import org.apache.flink.client.program.MiniClusterClient
import org.apache.flink.configuration.{ConfigConstants, Configuration, DeploymentOptions, JobManagerOptions, RestOptions, TaskManagerOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.data.conversion.DataStructureConverters
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.runtime.connector.sink.DataStructureConverterWrapper
import org.apache.flink.table.runtime.typeutils.{ExternalTypeInfo, InternalTypeInfo}
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils
import org.apache.flink.table.types.{DataType, FieldsDataType}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import SqlDateTypesSuite._
import scala.connector.log.LogSinkFunction
import scala.connector.test.TestCluster.{createLocalCluster, setJobManagerInfoToConfig}

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
   * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/common/#%E7%BF%BB%E8%AF%91%E4%B8%8E%E6%89%A7%E8%A1%8C%E6%9F%A5%E8%AF%A2
   * Table API 或者 SQL 查询在下列情况下会被翻译：
   * 当 TableEnvironment.executeSql() 被调用时。该方法是用来执行一个 SQL 语句，一旦该方法被调用， SQL 语句立即被翻译。
   * 当 Table.executeInsert() 被调用时。该方法是用来将一个表的内容插入到目标表中，一旦该方法被调用， TABLE API 程序立即被翻译。
   * 当 Table.execute() 被调用时。该方法是用来将一个表的内容收集到本地，一旦该方法被调用， TABLE API 程序立即被翻译。
   * 当 StatementSet.execute() 被调用时。Table （通过 StatementSet.addInsert() 输出给某个 Sink）和 INSERT 语句 （通过调用 StatementSet.addInsertSql()）会先被缓存到 StatementSet 中，StatementSet.execute() 方法被调用时，所有的 sink 会被优化成一张有向无环图。
   * 当 Table 被转换成 DataStream 时（参阅与 DataStream 集成）。转换完成后，它就成为一个普通的 DataStream 程序，并会在调用 StreamExecutionEnvironment.execute() 时被执行。
   *
   * insert的sql被翻译，就相当于提交了一个job
   */
  test("TranslateExecuteSqlQuery") {
    var sql =
      """
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

    sql =
      """
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

    Thread.sleep(1000 * 60 * 10)
  }

  /**
   * 端口绑定报错，要新建一个本地的环境
   * 本地模式只能通过sql或者ds一种方式提交，这是两次任务的提交
   */
  test("TranslateExecuteSqlQuery2") {
    var sql =
      """
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

    sql =
      """
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
  }


  /**
   * 把table转成ds，统一通过ds的env提交
   */
  test("TranslateExecuteSqlQuery3") {
    var sql =
      """
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

    import scala.sql.utils.TableImplicits._

    val rstTable = tEnv.sqlQuery("select * from tmp_tb1")
    rstTable.toAppendStream[RowData].addSink(new LogSinkFunction(
      "warn",
      rstTable.getJsonRowDataSerializationSchema
    ))

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.toDataStream.addSink(println(_))
  }

  /**
   * 模拟集群提交，可以看到提交了两个job，实际中要避免这种操作
   */
  test("TranslateExecuteSqlQueryMiniClusterClient") {
    val config = new Configuration()
    config.set(TaskManagerOptions.NUM_TASK_SLOTS, 2: Integer)
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

    var sql =
      """
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

    sql =
      """
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

  def printRowDataType(dataType: DataType): Unit = {
    val fieldsDataType = dataType.asInstanceOf[FieldsDataType]
    val logicalType = fieldsDataType.getLogicalType
    val fieldDataTypes = fieldsDataType.getChildren
    println("RowDataType logicalType:")
    println(logicalType.getClass.getSimpleName, logicalType)
    println("RowDataType fieldDataTypes:")
    fieldDataTypes.asScala.zip(logicalType.asInstanceOf[RowType].getFieldNames.asScala).foreach { case (dataType, name) =>
      println(s"name:$name,dataType:(${dataType.getClass.getSimpleName}, $dataType), logicalType:(${dataType.getLogicalType.getClass.getSimpleName},${dataType.getLogicalType}), logicalTypeRoot:${dataType.getLogicalType.getTypeRoot}")
    }
    println("-" * 20)

    // | sql               | DataType           | LogicalType             | LogicalTypeRoot                | default conversion class | internal conversion class |
    fieldDataTypes.asScala.zip(logicalType.asInstanceOf[RowType].getFieldNames.asScala).foreach { case (dataType, name) =>
      println(s"| ${dataType.getLogicalType.toString.toLowerCase} | ${dataType.getClass.getSimpleName} | ${dataType.getLogicalType.getClass.getSimpleName} | ${dataType.getLogicalType.getTypeRoot} | ${dataType.getLogicalType.getDefaultConversion.getSimpleName} | ${LogicalTypeUtils.toInternalConversionClass(dataType.getLogicalType).getSimpleName} |")
    }
  }

  /**
   *
   */
  test("sql_type_cat") {
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      price double,
      cnt bigint,
      data row<name string, age int>,
      name_set multiset<string>,
      names array<string>,
      datas array<row<name string, age int>>,
      value_counts map<string, int>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.price.expression' = '#{number.randomDouble ''2'',''1'',''150''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.name_set.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.name_set.length' = '4',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.value_counts.key.expression' = '#{harry_potter.spell}',
      'fields.value_counts.value.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.value_counts.length' = '3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    val sourceRowDataType = table.getResolvedSchema.toSourceRowDataType
    val physicalRowDataType = table.getResolvedSchema.toPhysicalRowDataType

    printRowDataType(sourceRowDataType)
    println("#" * 20)
    printRowDataType(physicalRowDataType)
    println("#" * 20)
    table.toDataStream.print()
  }

  test("class_to_RowData"){

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

    // scala类的typeinfo要使用scala隐式参数推导，TypeInformation.of(classOf[People])这种方式并不能解析的到
    //val ds = env.fromCollection(datas)(TypeInformation.of(classOf[People]))
    val ds = env.fromCollection(datas)

    //val inputTypeInfo = TypeInformation.of(classOf[People])
    val inputTypeInfo = implicitly[TypeInformation[People]]
    val inputDataType = TypeInfoDataTypeConverter.toDataType(tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory,
      inputTypeInfo)
    println(inputDataType)

    /**
     * new DataStructureConverterWrapper(DataStructureConverters.getConverter(producedDataType))
     *    org.apache.flink.table.runtime.connector.source.DataStructureConverterWrapper:
     *        structureConverter.toInternalOrNull(externalStructure)
     *    org.apache.flink.table.runtime.connector.sink.DataStructureConverterWrapper:
     *        structureConverter.toExternalOrNull(internalStructure)
     *
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

    //table.execute().print()
    table.toDataStream.addSink { row =>
      println(row)
    }
  }

  test("ds转table_Row") {
    val sql =
      """
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

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    val rowDs: DataStream[Row] = table.toDataStream
    println(rowDs.dataType)

    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.RowRowConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDs)


    rstTable.toDataStream.addSink { row =>
      println(row)
    }
  }

  test("ds转table_RowData") {
    val sql =
      """
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

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    val rowDataDataType: DataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs: DataStream[RowData] = table.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    val dataType = rowDataDs.dataType
    println(rowDataDs.dataType)

    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.IdentityConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDataDs)

    rstTable.toDataStream.addSink { row =>
      println(row)
    }
  }

  test("ds转table_RowData2") {
    val names = Array("莫南","璇音","青丝","流沙")

    val rowDataDs: DataStream[RowData] = env.fromSequence(1, 10000).map{ i =>
      if(i % 5 > 0){
        val name = names((i % 4).toInt)
        val age = java.lang.Integer.valueOf((18 + i % 100).toInt)
        val cnt = java.lang.Long.valueOf(18 + i % 100)
        val row: RowData = GenericRowData.of(StringData.fromString(name), age, cnt)
        row
      }else{
        GenericRowData.of(null, null, null)
      }
    }(ExternalTypeInfo.of(
      DataTypes.ROW(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("cnt", DataTypes.BIGINT())
      ).bridgedTo(classOf[RowData])
    ))

    println((ExternalTypeInfo.of(
      DataTypes.ROW(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("cnt", DataTypes.BIGINT())
      ).bridgedTo(classOf[RowData])
    )))
    val typeInformation: TypeInformation[RowData] = ExternalTypeInfo.of(
      tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
      .createDataType("ROW<name string, age int, cnt bigint>").bridgedTo(classOf[RowData]))
    println(typeInformation)

    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.IdentityConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDataDs)


    val rowDataDataType: DataType = rstTable.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs2: DataStream[RowData] = rstTable.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    println(rowDataDs2.dataType)

    rowDataDs2.addSink{ row =>
      println(row)
    }
  }

  test("ds转table_RowData3") {
    val names = Array("莫南","璇音","青丝","流沙")

    val rowDataDs: DataStream[RowData] = env.fromSequence(1, 10000).map{ i =>
      if(i % 5 > 0){
        val name = names((i % 4).toInt)
        val age = java.lang.Integer.valueOf((18 + i % 100).toInt)
        val cnt = java.lang.Long.valueOf(18 + i % 100)
        val row: RowData = GenericRowData.of(StringData.fromString(name), age, cnt)
        row
      }else{
        GenericRowData.of(null, null, null)
      }
    }(ExternalTypeInfo.of(
      tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
        .createDataType("ROW<name string, age int, cnt bigint>").bridgedTo(classOf[RowData]))
    )

    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.IdentityConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDataDs)


    val rowDataDataType: DataType = rstTable.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs2: DataStream[RowData] = rstTable.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    println(rowDataDs2.dataType)

    rowDataDs2.addSink{ row =>
      println(row)
    }
  }

  test("ds转table_RowData4") {
    val names = Array("莫南","璇音","青丝","流沙")

    val rowDataDs: DataStream[RowData] = env.fromSequence(1, 10000).map{ i =>
      if(i % 5 > 0){
        val name = names((i % 4).toInt)
        val age = java.lang.Integer.valueOf((18 + i % 100).toInt)
        val cnt = java.lang.Long.valueOf(18 + i % 100)
        val row: RowData = GenericRowData.of(StringData.fromString(name), age, cnt)
        row
      }else{
        GenericRowData.of(null, null, null)
      }
    }(InternalTypeInfo.of[RowData](
      tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
        .createDataType("ROW<name string, age int, cnt bigint>").getLogicalType)
    )


    /**
     * [[org.apache.flink.table.runtime.operators.source.InputConversionOperator]]
     * internalRecord = converter.toInternal(externalRecord)
     * [[org.apache.flink.table.data.conversion.IdentityConverter]]
     */
    val rstTable = tEnv.fromDataStream(rowDataDs)


    val rowDataDataType: DataType = rstTable.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs2: DataStream[RowData] = rstTable.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    val tape1 = InternalTypeInfo.of[RowData](
      tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
        .createDataType("ROW<name string, age int, cnt bigint>").getLogicalType)
    val tape2 = rowDataDs2.dataType
    println(tape1)
    println(tape2)
    println()

    rowDataDs2.addSink{ row =>
      println(row)
    }
  }

  test("toDataStream_append") {
    val sql =
      """
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

    val table = tEnv.sqlQuery("select * from tmp_tb1")

    val rowDs: DataStream[Row] = table.toDataStream
    println(rowDs.dataType)

    rowDs.addSink { row =>
      println(row)
    }
  }

  test("toDataStream_change") {
    val sql =
      """
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

    val table = tEnv.sqlQuery("select name, max(age) age from tmp_tb1 group by name")

    /**
     * 编译就直接报错了
     * doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[name], select=[name, MAX(age) AS age])
     * 可以使用toDataStream校验table是否有update changes操作
     */
    //val rowDs: DataStream[Row] = table.toDataStream
    val rowDs: DataStream[Row] = table.toChangelogStream
    println(rowDs.dataType)
    table.execute().print()

    rowDs.addSink { row =>
      println(row)
    }
  }

  test("table转ds_change") {
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select name, count(age) age from tmp_tb1 group by name")

    // api显示返回的就只是Row类型
    val rowDs: DataStream[Row] = table.toChangelogStream
    println(rowDs.dataType)


    /**
     * 不管是getRowDataDataStreamInternal还是toRetractStream[RowData]，返回的RowData都不是标准的RowData
     * 我说官方怎么没提供toChangelogStream的RowData版本
     */
    val rowDataDs: DataStream[RowData] = scala.connector.common.Utils.getRowDataDataStreamInternal(table, null)
    println(rowDataDs.dataType)

    /**
     * 做了转换：
     * [[org.apache.flink.table.runtime.operators.sink.OutputConversionOperator]]
     * [[org.apache.flink.table.runtime.connector.sink.DataStructureConverterWrapper]]
     * [[org.apache.flink.table.data.conversion.RowRowConverter]]
     */
    rowDs.addSink { row =>
      println(row)
    }

    /**
     * 这个RowData是
     * org.apache.flink.table.data.utils.JoinedRowData
     * 可以通过getString等api正常访问属性，可以看
     * [[org.apache.flink.table.data.utils.JoinedRowData#getString(int)]]
     * 这个转换是[[org.apache.flink.table.data.conversion.IdentityConverter]]
     * 性能损失降到最小
     */
    rowDataDs.addSink { row =>
      println(row)
      row.getString(0)
      row.getLong(1)
      println(row.getRowKind, row.getString(0), row.getLong(1))
    }
    table.toRetractStream[RowData].addSink { row =>
      println(row)
    }
  }

  test("table转ds") {
    val sql =
      """
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
     * // include all columns of the query (incl. metadata and computed columns)
     * val sourceType = table.getResolvedSchema.toSourceRowDataType
     * toDataStream(table, sourceType)
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

    rowDataDs.addSink { row =>
      println(row)
    }
  }

  test("table转ds2") {
    val sql =
      """
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
     * 把RowData转成外部类型
     */
    val dataType = DataTypes.of(classOf[TableCaseData]).toDataType(tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory)
    println(dataType)
    val structureConverter = DataStructureConverters.getConverter(dataType)
    //new DataStructureConverterWrapper(DataStructureConverters.getConverter(dataType))

    /**
     * table转ds会加一个filter校验过滤非空属性，默认data中的非空属性table这行实际的列值为null时会抛出异常
     * 设置'table.exec.sink.not-null-enforcer'='drop'后，会直接把这一行给直接过滤掉，肯定不能设置这个
     * 这个是运行时报错的，要是实际就没null，转的data中使用原生类型也是不会报错的。不是编译阶段报错
     * org.apache.flink.table.runtime.operators.sink.SinkNotNullEnforcer
     *
     * 实际相当于调用：toDataStream(table, DataTypes.of(targetClass))
     */
    val dataDs = table.toDataStream(classOf[TableCaseData])
    println(dataDs.dataType)
    //dataDs.print()
    dataDs.addSink { row =>
      println(row)
    }

  }

  test("table_execute_print") {
    val sql =
      """
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

  test("timestamp_convert") {
    val sql =
      """
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
     * LocalDateTime：直接把时间戳传进去创建，LocalDateTime这玩意里面的时间戳似乎是没时区感知的
     * Timestamp：使用TimestampTimestampConverter，Timestamp.valueOf(toLocalDateTime())，直接把LocalDateTime的year,month...传进去了，相当于utc的时间
     *
     * datetime_ltz的转化：
     * Instant：相当于直接返回时间戳
     * Timestamp：使用LocalZonedTimestampTimestampConverter，直接把时间戳传进去了，有时区感知
     * Long：直接返回时间戳
     */
    rowDs.addSink { row =>
      println(row)
    }

  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}

object SqlDateTypesSuite {

  case class People(id: Long, name: String, age: Int, score: Double, event_time: Instant)

  case class TableCaseSubData(name: String, age: Integer)

  //case class TableCaseData(name: String, age: Integer, cnt: java.lang.Long, data: TableCaseSubData, datas:java.util.List[TableCaseSubData])
  //case class TableCaseData(name: String, age: Integer, cnt: java.lang.Long, data: TableCaseSubData, datas:Array[TableCaseSubData])

  case class TableCaseData(name: String, age: Int, cnt: java.lang.Long, data: TableCaseSubData, datas: Array[TableCaseSubData])

}
