package scala.sql.utils

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.json.{JsonFormatOptions, JsonRowDataSerializationSchema, JsonRowSerializationSchema}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.TableConversions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.reflect.ClassTag
import scala.serialization.SerializationSchemaLogWrapper

object TableImplicits {
  implicit class TableOps(table: Table) {
    def toRetractStreamOnlyAdd[T: TypeInformation]: DataStream[T] = {
      new TableConversions(table).toRetractStream[T].flatMap(new FlatMapFunction[(Boolean, T), T] {
        override def flatMap(value: (Boolean, T), out: Collector[T]): Unit = {
          if (value._1) {
            out.collect(value._2)
          }
        }
      })
    }

    def getJsonRowDataSerializationSchema: SerializationSchema[RowData] = {
      val rowType = table.getResolvedSchema.toPhysicalRowDataType.getLogicalType
      val serializer = new JsonRowDataSerializationSchema(
        rowType.asInstanceOf[RowType], TimestampFormat.SQL,JsonFormatOptions.MapNullKeyMode.FAIL, "null", true)
      serializer
    }

    def getJsonRowSerializationSchema: SerializationSchema[Row] = {
      val dataType = TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(table.getResolvedSchema.toPhysicalRowDataType).asInstanceOf[TypeInformation[Row]]
      val serializer = JsonRowSerializationSchema.builder().withTypeInfo(dataType).build()
      serializer
    }

  }

  implicit class SerializationSchemaOps[T:ClassTag](serializer: SerializationSchema[T]){
    def wrapLog: SerializationSchema[T] ={
      new SerializationSchemaLogWrapper(serializer)
    }
  }
}
