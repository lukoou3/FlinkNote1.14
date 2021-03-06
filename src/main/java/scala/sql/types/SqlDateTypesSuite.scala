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
   * Table API ?????? SQL ???????????????????????????????????????
   * ??? TableEnvironment.executeSql() ????????????????????????????????????????????? SQL ???????????????????????????????????? SQL ????????????????????????
   * ??? Table.executeInsert() ????????????????????????????????????????????????????????????????????????????????????????????????????????? TABLE API ????????????????????????
   * ??? Table.execute() ??????????????????????????????????????????????????????????????????????????????????????????????????? TABLE API ????????????????????????
   * ??? StatementSet.execute() ???????????????Table ????????? StatementSet.addInsert() ??????????????? Sink?????? INSERT ?????? ??????????????? StatementSet.addInsertSql()????????????????????? StatementSet ??????StatementSet.execute() ?????????????????????????????? sink ???????????????????????????????????????
   * ??? Table ???????????? DataStream ??????????????? DataStream ????????????????????????????????????????????????????????? DataStream ???????????????????????? StreamExecutionEnvironment.execute() ???????????????
   *
   * insert???sql???????????????????????????????????????job
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
   * ???????????????????????????????????????????????????
   * ????????????????????????sql??????ds????????????????????????????????????????????????
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
   * ???table??????ds???????????????ds???env??????
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
   * ????????????????????????????????????????????????job?????????????????????????????????
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
      'fields.name_set.expression' = '#{regexify ''(??????|??????|??????|??????){1}''}',
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
   * ?????????????????????????????????????????????????????????????????????/???????????????????????????????????????
   * ?????????????????????????????????????????????????????????????????????fromDataStream(stream, $"myLong", $"myString")/toAppendStream/toRetractStream
   * ?????????????????????????????????fromDataStream/toDataStream, fromChangelogStream/toChangelogStream
   * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/data_stream_api/#examples-for-fromdatastream
   * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/data_stream_api/#legacy-conversion
   */
  test("ds???table") {
    val datas = Seq(
      People(1, "aaa", 20, 90.2, Instant.ofEpochMilli(1000)),
      People(2, "bb", 22, 90.2, Instant.ofEpochMilli(1001)),
      People(3, "cc", 21, 90.2, Instant.ofEpochMilli(1002))
    )

    // scala??????typeinfo?????????scala?????????????????????TypeInformation.of(classOf[People])?????????????????????????????????
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
     * ?????????????????????physical columns
     * ds???.TypeInfo???sql???DataType?????????????????????[[org.apache.flink.table.types.utils.TypeInfoDataTypeConverter.conversionMap]]
     * Instant??????TIMESTAMP_LTZ
     * sql???????????????????????????????????????LogicalTypeUtils.toInternalConversionClass
     * sql???????????????????????????????????????????????????connector??????????????????json??????????????????????????????JsonToRowDataConverters???RowDataToJsonConverters
     */
    var table = tEnv.fromDataStream(ds)
    table.printSchema()

    /**
     * ?????????????????????physical columns
     * ?????????????????????
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

  test("ds???table_Row") {
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

  test("ds???table_RowData") {
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

  test("ds???table_RowData2") {
    val names = Array("??????","??????","??????","??????")

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

  test("ds???table_RowData3") {
    val names = Array("??????","??????","??????","??????")

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

  test("ds???table_RowData4") {
    val names = Array("??????","??????","??????","??????")

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
     * ????????????????????????
     * doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[name], select=[name, MAX(age) AS age])
     * ????????????toDataStream??????table?????????update changes??????
     */
    //val rowDs: DataStream[Row] = table.toDataStream
    val rowDs: DataStream[Row] = table.toChangelogStream
    println(rowDs.dataType)
    table.execute().print()

    rowDs.addSink { row =>
      println(row)
    }
  }

  test("table???ds_change") {
    val sql =
      """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{regexify ''(??????|??????|??????|??????){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select name, count(age) age from tmp_tb1 group by name")

    // api????????????????????????Row??????
    val rowDs: DataStream[Row] = table.toChangelogStream
    println(rowDs.dataType)


    /**
     * ?????????getRowDataDataStreamInternal??????toRetractStream[RowData]????????????RowData??????????????????RowData
     * ???????????????????????????toChangelogStream???RowData??????
     */
    val rowDataDs: DataStream[RowData] = scala.connector.common.Utils.getRowDataDataStreamInternal(table, null)
    println(rowDataDs.dataType)

    /**
     * ???????????????
     * [[org.apache.flink.table.runtime.operators.sink.OutputConversionOperator]]
     * [[org.apache.flink.table.runtime.connector.sink.DataStructureConverterWrapper]]
     * [[org.apache.flink.table.data.conversion.RowRowConverter]]
     */
    rowDs.addSink { row =>
      println(row)
    }

    /**
     * ??????RowData???
     * org.apache.flink.table.data.utils.JoinedRowData
     * ????????????getString???api??????????????????????????????
     * [[org.apache.flink.table.data.utils.JoinedRowData#getString(int)]]
     * ???????????????[[org.apache.flink.table.data.conversion.IdentityConverter]]
     * ????????????????????????
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

  test("table???ds") {
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
     * ???????????????????????? [[StreamTableEnvironment.toDataStream(Table)]]
     * ???????????????????????????[[org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl.toDataStream(Table)]]
     * ???????????????????????????
     * // include all columns of the query (incl. metadata and computed columns)
     * val sourceType = table.getResolvedSchema.toSourceRowDataType
     * toDataStream(table, sourceType)
     * ??????bridged???RowData????????????????????????Row??????
     */
    val rowDs: DataStream[Row] = table.toDataStream
    println(rowDs.dataType)


    /**
     * ???RowData??????????????????????????????????????????????????????
     * ??????toAppendStream[RowData]??????api?????????????????????RowData???
     */
    val rowDataDataType: DataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs: DataStream[RowData] = table.toDataStream[RowData](rowDataDataType)
    //val rowDataDs: DataStream[RowData] = table.toAppendStream[RowData]
    println(rowDataDs.dataType)

    rowDataDs.addSink { row =>
      println(row)
    }
  }

  test("table???ds2") {
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
     * ???RowData??????????????????
     */
    val dataType = DataTypes.of(classOf[TableCaseData]).toDataType(tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory)
    println(dataType)
    val structureConverter = DataStructureConverters.getConverter(dataType)
    //new DataStructureConverterWrapper(DataStructureConverters.getConverter(dataType))

    /**
     * table???ds????????????filter?????????????????????????????????data??????????????????table????????????????????????null??????????????????
     * ??????'table.exec.sink.not-null-enforcer'='drop'????????????????????????????????????????????????????????????????????????
     * ????????????????????????????????????????????????null?????????data?????????????????????????????????????????????????????????????????????
     * org.apache.flink.table.runtime.operators.sink.SinkNotNullEnforcer
     *
     * ????????????????????????toDataStream(table, DataTypes.of(targetClass))
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
     * ??????????????????????????????org.apache.flink.table.utils.PrintUtils#rowToString()
     * ?????????????????????????????????
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
     * ?????????operator???OutputConversionOperator????????????????????????RowRowConverter
     * ??????RowRowConverter????????????[[org.apache.flink.table.data.conversion.RowRowConverter.create]]
     * ????????????????????????[[org.apache.flink.table.data.conversion.DataStructureConverters.getConverter]]
     * ?????????????????????map: [[org.apache.flink.table.data.conversion.DataStructureConverters.converters]]
     * timestamp????????????
     * LocalDateTime???????????????????????????????????????LocalDateTime??????????????????????????????????????????????????????
     * Timestamp?????????TimestampTimestampConverter???Timestamp.valueOf(toLocalDateTime())????????????LocalDateTime???year,month...????????????????????????utc?????????
     *
     * datetime_ltz????????????
     * Instant?????????????????????????????????
     * Timestamp?????????LocalZonedTimestampTimestampConverter???????????????????????????????????????????????????
     * Long????????????????????????
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
