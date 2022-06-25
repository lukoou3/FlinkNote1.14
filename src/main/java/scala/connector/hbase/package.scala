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
import scala.connector.common.Utils

package object hbase {
  type HbaseType = Array[Byte]
  type Getter = RowData => HbaseType

  implicit class TableFunctions(table: Table){
    def addRowDataBatchIntervalEsSink(
      params: HbaseSinkParams
    ): DataStreamSink[RowData] = {
      val sink = getRowDataBatchIntervalHbaseSink(table.getResolvedSchema, params)
      val rowDataDs = table.toDataStream[RowData](table.getResolvedSchema.toSourceRowDataType.bridgedTo(classOf[RowData]))
      rowDataDs.addSink(sink)
    }
  }

  case class HbaseSinkParams(
    hbaseConf: Map[String, String],
    tableName: String,
    cf: String,
    rowKey: String = "rowkey",
    qualifiers: Seq[String] = Nil,
    fieldColMap: Map[String, String] = Map.empty,
    onlyStringCol:Boolean = true,
    batchSize: Int = 500,
    batchIntervalMs: Long = 1000L,
    minPauseBetweenFlushMs: Long = 100L,
    keyedMode: Boolean = false,
    keys: Seq[String] = Nil,
    orderBy: Seq[(String, Boolean)] = Nil
  )

  def getRowDataBatchIntervalHbaseSink(
    resolvedSchema: ResolvedSchema,
    params: HbaseSinkParams
  ): BatchIntervalHbaseSink[RowData] = {
    val typeInformation: InternalTypeInfo[RowData] = InternalTypeInfo.of(resolvedSchema.toSourceRowDataType.getLogicalType)
    val family = Bytes.toBytes(params.cf)
    if(params.qualifiers.nonEmpty){
      assert(resolvedSchema.getColumns.asScala.map(col => params.fieldColMap.getOrElse(col.getName, col.getName))
        .filter(params.rowKey != _).forall(params.qualifiers.contains(_)), s"列名必须在${params.qualifiers}中")
    }
    val (rkFields, colFields) = resolvedSchema.getColumns.asScala.zipWithIndex.partition(params.rowKey == _._1.getName)
    val rkFieldGetter = rkFields.map{ case(col, i) =>
      val logicalType = col.getDataType.getLogicalType
      assert(Seq(CHAR, VARCHAR, BINARY).contains(logicalType.getTypeRoot), "rowkey col必须是string或者字节数组")
      makeGetter(logicalType, i)
    }.headOption.getOrElse(throw new Exception("没有rowkey col"))
    val colFieldGetters = colFields.map{ case(col, i) =>
      val name = col.getName
      val logicalType = col.getDataType.getLogicalType
      assert(!params.onlyStringCol || Seq(CHAR, VARCHAR).contains(logicalType.getTypeRoot), "只支持string列模式")
      (family, Bytes.toBytes(params.fieldColMap.getOrElse(name, name)), makeGetter(logicalType, i))
    }
    val _getKey = Utils.getTableKeyFunction(resolvedSchema,params.keyedMode,params.keys,params.orderBy)
    val tableOrdering = Utils.getTableOrdering(resolvedSchema, params.orderBy)

    new BatchIntervalHbaseSink[RowData](params.hbaseConf, params.tableName, params.batchSize, params.batchIntervalMs, params.minPauseBetweenFlushMs){
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

      override def getKey(data: RowData): Any = _getKey(data)

      override def replaceValue(newValue: RowData, oldValue: RowData): RowData = if (!this.keyedMode) {
        super.replaceValue(newValue, oldValue)
      } else {
        if (tableOrdering.gteq(newValue, oldValue)) {
          newValue
        } else {
          oldValue
        }
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
