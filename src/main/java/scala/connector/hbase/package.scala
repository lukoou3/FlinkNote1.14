package scala.connector

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, CHAR, DOUBLE, FLOAT, INTEGER, VARCHAR}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

package object hbase {
  type HbaseType = Array[Byte]
  type Getter = RowData => HbaseType

  def getRowDataBatchIntervalHbaseSink(
    resolvedSchema: ResolvedSchema,
    hbaseConf: Map[String, String],
    tableName: String,
    batchSize: Int,
    batchIntervalMs: Long,
    minPauseBetweenFlushMs: Long = 100L
  ): BatchIntervalHbaseSink[RowData]= {
    val typeInformation: InternalTypeInfo[RowData] = InternalTypeInfo.of(resolvedSchema.toSourceRowDataType.getLogicalType)



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

      override def data2EsPut(data: RowData): Put = {
        null
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
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
    }
  }
}
