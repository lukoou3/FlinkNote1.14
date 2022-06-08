package scala.connector

import java.util

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{ArrayType, LogicalType, RowType}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

package object es {

  def getRowDataBatchIntervalJdbcSink(
    resolvedSchema: ResolvedSchema,
    cfg: Map[String, String],
    batchSize: Int,
    batchIntervalMs: Long,
    minPauseBetweenFlushMs: Long = 100L
  ): BatchIntervalEsSink[RowData, util.Map[_, _]]= {

    new BatchIntervalEsSink[RowData, util.Map[_, _]](cfg, batchSize, batchIntervalMs, minPauseBetweenFlushMs){
      def data2EsRecord(data: RowData): util.Map[_, _] = {
        null
      }
    }
  }

  def fieldGeterFunc(logicalType: LogicalType): AnyRef => AnyRef = {
    logicalType.getTypeRoot match {
      case CHAR | VARCHAR => identity
      case INTEGER => identity
      case BIGINT => identity
      case FLOAT => identity
      case DOUBLE => identity
      case ROW =>
        val fieldGeters = logicalType.asInstanceOf[RowType].getFields.asScala.zipWithIndex.map { case (col, i) =>
          (i, col.getName, fieldGeterFunc(col.getType))
        }
        x => {
          if(x != null){
            val row = x.asInstanceOf[Row]
            val map = new util.HashMap[String, AnyRef]()
            fieldGeters.foreach{ case (i, name, valueGetter) =>
              map.put(name, valueGetter(row.getField(i)))
            }
            map
          }else{
            null
          }
        }
      case ARRAY =>
        val valueGetter = fieldGeterFunc(logicalType.asInstanceOf[ArrayType].getElementType)
        x => {
          if(x != null){
            val list = new util.ArrayList[AnyRef]()
            for (elem <- x.asInstanceOf[Array[AnyRef]]) {
              list.add(valueGetter(elem))
            }
            list
          }else{
            null
          }
        }
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
    }
  }
}
