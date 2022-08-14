package scala.sql.utils

import java.util

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.json.{JsonFormatOptions, JsonRowDataDeserializationSchema, JsonRowDataSerializationSchema, JsonRowSerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Expressions, Schema, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.internal.TableImpl
import org.apache.flink.table.data.RowData
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.runtime.typeutils.{ExternalTypeInfo, InternalTypeInfo}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, CHAR, DOUBLE, FLOAT, INTEGER, ROW, VARCHAR}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector

import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import scala.serialization.SerializationSchemaLogWrapper
import scala.stream.func.DeserializeFunc

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
        rowType.asInstanceOf[RowType], TimestampFormat.SQL, JsonFormatOptions.MapNullKeyMode.FAIL, "null", true)
      serializer
    }

    def getJsonRowSerializationSchema: SerializationSchema[Row] = {
      val dataType = TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(table.getResolvedSchema.toPhysicalRowDataType).asInstanceOf[TypeInformation[Row]]
      val serializer = JsonRowSerializationSchema.builder().withTypeInfo(dataType).build()
      serializer
    }

    def toJsonDataStream(streamType: String = "Append"): DataStream[String] = {
      val internalEnv = table.asInstanceOf[TableImpl].getTableEnvironment

      val fieldGeters = table.getSchema.getTableColumns.asScala.zipWithIndex.map { case (col, i) =>
        println(i, col.getName, col.getType.getLogicalType.getTypeRoot, col.getType, col.getType.getLogicalType)
        val func: AnyRef => AnyRef = col.getType.getLogicalType.getTypeRoot match {
          case CHAR | VARCHAR => x => x
          case INTEGER => x => x
          case BIGINT => x => x
          case FLOAT => x => x
          case DOUBLE => x => x
          case _ => throw new UnsupportedOperationException(s"unsupported data type $col.getType.getLogicalType.getTypeRoot")
        }
        (i, col.getName, func)
      }
      val ds = internalEnv match {
        case tableEnv: StreamTableEnvironment =>
          streamType match {
            case "Append" => new TableConversions(table).toAppendStream[Row]
            case "Retract" => new TableConversions(table).toRetractStream[Row].flatMap(new FlatMapFunction[(Boolean, Row), Row] {
              override def flatMap(value: (Boolean, Row), out: Collector[Row]): Unit = {
                val row = value._2
                if (row.getKind == RowKind.INSERT || row.getKind == RowKind.UPDATE_AFTER) {
                  out.collect(row)
                }
              }
            })
          }
        case tableEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment =>
          new DataStream(tableEnv.toDataStream(table))
      }

      ds.map(new RichMapFunction[Row, String] {
        val map = new util.HashMap[String, AnyRef]()

        override def map(row: Row): String = {
          map.clear()

          for ((i, name, valueGetter) <- fieldGeters) {
            val v = valueGetter(row.getField(i))
            map.put(name, v)
          }

          JSON.toJSONString(map, false)
        }
      })
    }
  }

  implicit class SerializationSchemaOps[T: ClassTag](serializer: SerializationSchema[T]) {
    def wrapLog: SerializationSchema[T] = {
      new SerializationSchemaLogWrapper(serializer)
    }
  }

  implicit class StreamTableEnvOps(tEnv: StreamTableEnvironment) {
    def createDataType(schema: String, internal:Boolean = false): DataType ={
      val dType = tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager.getDataTypeFactory.createDataType(schema)
      if(!internal){
        dType
      }else{
        val clazz = dType.getLogicalType.getTypeRoot match {
          case ROW => classOf[RowData]
          case _ => throw new UnsupportedOperationException(s"unsupported data type ${dType.getLogicalType.getTypeRoot}")
        }
        dType.bridgedTo(clazz)
      }
    }

    def createTemporaryViewFromProductDs[T <: Product](name: String, ds: DataStream[T], useProctime: Boolean = true, proctimeName: Option[String] = None): Unit = {
      if (!useProctime) {
        tEnv.createTemporaryView(name, ds)
      } else {
        tEnv.createTemporaryView(name, ds,
          Schema.newBuilder()
            .columnByExpression(proctimeName.getOrElse("proctime"), "PROCTIME()")
            .build()
        )
      }
    }
  }

  implicit class DataStreamTableOps[T](ds: DataStream[T]) {
    def createTemporaryViewUseJsonFormat(tEnv: StreamTableEnvironment, path: String, schema: String,
      failOnMissingField:Boolean = false, ignoreParseErrors:Boolean = true,
      useProctime: Boolean = true, proctimeName: Option[String] = None): Unit = {
      // 这里判断，应用编译阶段就能发现类型不匹配的
      assert(ds.dataType eq PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)

      val dataType = tEnv.createDataType(schema, true)
      assert(dataType.getLogicalType.isInstanceOf[RowType], "schema必须是row类型")

      val rowType = dataType.getLogicalType.asInstanceOf[RowType]
      val rowDataTypeInfo = InternalTypeInfo.of[RowData](dataType.getLogicalType) // ExternalTypeInfo.of(dataType)

      val deserializer = new JsonRowDataDeserializationSchema(rowType, rowDataTypeInfo, failOnMissingField, ignoreParseErrors, TimestampFormat.SQL)
      val rowDS: DataStream[RowData] = ds.asInstanceOf[DataStream[Array[Byte]]].map(new DeserializeFunc(deserializer))(rowDataTypeInfo)

      if (!useProctime) {
        tEnv.createTemporaryView(path, rowDS)
      } else {
        tEnv.createTemporaryView(path, rowDS,
          Schema.newBuilder()
            .columnByExpression(proctimeName.getOrElse("proctime"), "PROCTIME()")
            .build()
        )
      }
    }
  }

}
