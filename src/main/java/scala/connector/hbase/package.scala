package scala.connector

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

package object hbase {
  type HbaseType = Array[Byte]
  type Getter = RowData => HbaseType

  implicit class TableFunctions(table: Table){
    def addRowDataBatchIntervalEsSink(
      hbaseConf: Map[String, String],
      tableName: String,
      cf: String,
      rowKey: String = "rowkey",
      qualifiers: Seq[String] = Nil,
      fieldColMap: Map[String, String] = Map.empty,
      onlyStringCol:Boolean = true,
      batchSize: Int = 500,
      batchIntervalMs: Long = 1000L,
      minPauseBetweenFlushMs: Long = 100L
    ): DataStreamSink[RowData] = {
      val sink = getRowDataBatchIntervalHbaseSink(table.getResolvedSchema, hbaseConf, tableName, cf,
        rowKey=rowKey, qualifiers=qualifiers, fieldColMap=fieldColMap,onlyStringCol=onlyStringCol,
        batchSize=batchSize, batchIntervalMs=batchIntervalMs, minPauseBetweenFlushMs=minPauseBetweenFlushMs)
      val rowDataDs = table.toDataStream[RowData](table.getResolvedSchema.toSourceRowDataType.bridgedTo(classOf[RowData]))
      rowDataDs.addSink(sink)
    }
  }

  def getRowDataBatchIntervalHbaseSink(
    resolvedSchema: ResolvedSchema,
    hbaseConf: Map[String, String],
    tableName: String,
    cf: String,
    rowKey: String = "rowkey",
    qualifiers: Seq[String] = Nil,
    fieldColMap: Map[String, String] = Map.empty,
    onlyStringCol:Boolean = true,
    batchSize: Int = 500,
    batchIntervalMs: Long = 1000L,
    minPauseBetweenFlushMs: Long = 100L
  ): BatchIntervalHbaseSink[RowData] = {
    val typeInformation: InternalTypeInfo[RowData] = InternalTypeInfo.of(resolvedSchema.toSourceRowDataType.getLogicalType)
    val family = Bytes.toBytes(cf)
    if(qualifiers.nonEmpty){
      assert(resolvedSchema.getColumns.asScala.map(col => fieldColMap.getOrElse(col.getName, col.getName)).filter(rowKey != _).forall(qualifiers.contains(_)), s"列名必须在${qualifiers}中")
    }
    val (rkFields, colFields) = resolvedSchema.getColumns.asScala.zipWithIndex.partition(rowKey == _._1.getName)
    val rkFieldGetter = rkFields.map{ case(col, i) =>
      val logicalType = col.getDataType.getLogicalType
      assert(Seq(CHAR, VARCHAR, BINARY).contains(logicalType.getTypeRoot), "rowkey col必须是string或者字节数组")
      makeGetter(logicalType, i)
    }.headOption.getOrElse(throw new Exception("没有rowkey col"))
    val colFieldGetters = colFields.map{ case(col, i) =>
      val name = col.getName
      val logicalType = col.getDataType.getLogicalType
      assert(!onlyStringCol || Seq(CHAR, VARCHAR).contains(logicalType.getTypeRoot), "只支持string列模式")
      (family, Bytes.toBytes(fieldColMap.getOrElse(name, name)), makeGetter(logicalType, i))
    }

    new BatchIntervalHbaseSink[RowData](hbaseConf, tableName, batchSize, batchIntervalMs, minPauseBetweenFlushMs){
      @transient var serializer: TypeSerializer[RowData] = _
      @transient var objectReuse = false

      override def onInit(parameters: Configuration): Unit = {
        super.onInit(parameters)
        objectReuse = getRuntimeContext.getExecutionConfig.isObjectReuseEnabled
        if(objectReuse){
          serializer = typeInformation.createSerializer(getRuntimeContext.getExecutionConfig)
        }
      }

      override def valueTransform(data: RowData): RowData = {
        if(objectReuse) serializer.copy(data) else data
      }

      override def data2EsPut(row: RowData): Put = {
        val put = new Put(rkFieldGetter(row))
        for ((family, qualifier, getter) <- colFieldGetters) {
          val value = getter(row)
          if(value != null){
            put.addColumn(family, qualifier, value)
          }
        }
        put
      }
    }
  }

  def makeGetter(logicalType: LogicalType, pos: Int): Getter = {
    logicalType.getTypeRoot match {
      case CHAR | VARCHAR => row => if (row.isNullAt(pos)) null else row.getString(pos).toBytes
      case INTEGER => row => if (row.isNullAt(pos)) null else Bytes.toBytes(row.getInt(pos))
      case BIGINT => row => if (row.isNullAt(pos)) null else Bytes.toBytes(row.getLong(pos))
      case FLOAT => row => if (row.isNullAt(pos)) null else Bytes.toBytes(row.getFloat(pos))
      case DOUBLE => row => if (row.isNullAt(pos)) null else Bytes.toBytes(row.getDouble(pos))
      case BINARY => row => if (row.isNullAt(pos)) null else row.getBinary(pos)
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
    }
  }
}
