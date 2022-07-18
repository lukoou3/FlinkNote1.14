package scala.file.sql

import java.nio.charset.StandardCharsets
import java.util
import java.util.List

import org.apache.flink.configuration.Configuration
import org.apache.flink.orc.shim.OrcShim
import org.apache.flink.orc.{OrcColumnarRowFileInputFormat, OrcFileFormatFactory, OrcFilters}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.vector.VectorizedColumnBatch
import org.apache.flink.table.filesystem.PartitionFieldExtractor
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcFile, TypeDescription}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.sql.utils.TableImplicits._

/**
 * orc官方api
 * https://orc.apache.org/docs/core-java.html
 *
 * NoClassDefFoundError: org/apache/log4j/Level
 * 引入：
 * <dependency>
 * <groupId>log4j</groupId>
 * <artifactId>log4j</artifactId>
 * <version>1.2.17</version>
 * </dependency>
 */
class OrcFileSuite extends AnyFunSuite with BeforeAndAfterAll{
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

  /***
   * filesystem实现还不支持Lookup join
   */
  test("connector_filesystem_Lookupread"){
    var sql = """
    CREATE TABLE tmp_tb (
      name string,
      province_id bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.province_id.expression' = '#{number.numberBetween ''0'',''40''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    CREATE TABLE fs_table_orc (
      province_id bigint PRIMARY KEY,
      province_name string
    ) WITH (
      'connector'='filesystem',
      'path'='file:///D:/ChromeDownload/dim_common_province_a.orc',
      'format'='orc'
    )
    """
    sql = """
    CREATE TABLE fs_table_orc (
      province_id bigint,
      province_name string
    ) WITH (
      'connector'='faker',
      'fields.province_name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.province_id.expression' = '#{number.numberBetween ''0'',''40''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        a.*,
        b.province_name
    from tmp_tb a
    left join fs_table_orc for system_time as of a.proctime as b on a.province_id = b.province_id
    """
    val table = tEnv.sqlQuery(sql)
    table.printSchema()

    val rowDataDataType: DataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs: DataStream[RowData] = table.toDataStream[RowData](rowDataDataType)

    val serializer = table.getJsonRowDataSerializationSchema
    rowDataDs.addSink{row =>
      println(new String(serializer.serialize(row), StandardCharsets.UTF_8))
    }

  }

  test("connector_filesystem_read"){
    var sql = """
    CREATE TABLE fs_table_orc (
      -- province_id bigint,
      province_name string
    ) WITH (
      'connector'='filesystem',
      'path'='file:///D:/ChromeDownload/dim_common_province_a.orc',
      'format'='orc'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        *
    from fs_table_orc
    """
    val table = tEnv.sqlQuery(sql)
    table.printSchema()

    val rowDataDataType: DataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs: DataStream[RowData] = table.toDataStream[RowData](rowDataDataType)

    val serializer = table.getJsonRowDataSerializationSchema
    rowDataDs.addSink{row =>
      println(new String(serializer.serialize(row), StandardCharsets.UTF_8))
    }

  }

  test("env_filesystem_read"){
    val path = "file:///D:/chromedownload/pin_ord_stat.snappy.orc"

    val rowType = tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory
      .createDataType("ROW<province_id bigint,province_name string>").getLogicalType.asInstanceOf[RowType]

    val format = OrcColumnarRowFileInputFormat.createPartitionedFormat(
      OrcShim.defaultShim,
      new org.apache.hadoop.conf.Configuration(),
      rowType,
      new util.ArrayList[String](),
      PartitionFieldExtractor.forFileSystem("__DEFAULT_PARTITION__"),
      Array(0, 1),
      new util.ArrayList[OrcFilters.Predicate],
      VectorizedColumnBatch.DEFAULT_SIZE
    )


    // orc的FileInputFormat这个版本没有了
    //val ds = env.readFile(format, path)

  }




  override protected def afterAll(): Unit = {
    env.execute()
  }
}
