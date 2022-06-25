package scala.connector

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.{ArrayData, RowData}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{ArrayType, LogicalType, MapType, RowType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.connector.common.Utils
import scala.log.Logging

package object es extends Logging {

  implicit class ProductDataStreamEsFunctions[T <: Product : TypeInformation](ds: DataStream[T]) {
    def addBatchIntervalEsSink(
      cfg: Map[String, String],
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      updateScriptOrderBy: Seq[(String, Boolean)] = Nil
    ): DataStreamSink[T] = {
      val productTypeInformation = implicitly[TypeInformation[T]].asInstanceOf[CaseClassTypeInfo[T]]
      val extraCfg = mutable.HashMap[String, String]()
      if (updateScriptOrderBy.nonEmpty) {
        extraCfg ++= getUpdateScriptCfg(productTypeInformation.fieldNames, cfg, updateScriptOrderBy)
      }
      ds.addSink(new BatchIntervalEsSink[T, T](cfg, batchSize, batchIntervalMs, minPauseBetweenFlushMs) {
        def data2EsRecord(data: T): T = data
      })
    }
  }

  implicit class TableFunctions(table: Table) {
    def addRowDataBatchIntervalEsSink(
      params: EsSinkParams
    ): DataStreamSink[RowData] = {
      val sink = getRowDataBatchIntervalEsSink(table.getResolvedSchema, params)
      val rowDataDs = table.toDataStream[RowData](table.getResolvedSchema.toSourceRowDataType.bridgedTo(classOf[RowData]))
      rowDataDs.addSink(sink)
    }
  }

  case class EsSinkParams(
    cfg: Map[String, String],
    batchSize: Int,
    batchIntervalMs: Long,
    minPauseBetweenFlushMs: Long = 100L,
    keyedMode: Boolean = false,
    keys: Seq[String] = Nil,
    orderBy: Seq[(String, Boolean)] = Nil,
    updateScriptOrderBy: Seq[(String, Boolean)] = Nil
  )

  def getRowDataBatchIntervalEsSink(
    resolvedSchema: ResolvedSchema,
    params: EsSinkParams
  ): BatchIntervalEsSink[RowData, util.Map[_, _]] = {
    val extraCfg = mutable.HashMap[String, String](
      ES_SERIALIZATION_WRITER_VALUE_CLASS -> classOf[JdkValueWriter].getName
    )
    if (params.updateScriptOrderBy.nonEmpty) {
      extraCfg ++= getUpdateScriptCfg(resolvedSchema.getColumns.asScala.map(_.getName), params.cfg, params.updateScriptOrderBy)
    }
    val typeInformation: InternalTypeInfo[RowData] = InternalTypeInfo.of(resolvedSchema.toSourceRowDataType.getLogicalType)
    val fieldGetters = resolvedSchema.getColumns.asScala.zipWithIndex.map { case (col, i) =>
      (i, col.getName, makeGetter(col.getDataType.getLogicalType))
    }
    val _getKey = Utils.getTableKeyFunction(resolvedSchema, params.keyedMode, params.keys, params.orderBy)
    val tableOrdering = Utils.getTableOrdering(resolvedSchema, params.orderBy)

    new BatchIntervalEsSink[RowData, util.Map[_, _]](params.cfg ++ extraCfg, params.batchSize, params.batchIntervalMs, params.minPauseBetweenFlushMs) {
      @transient var serializer: TypeSerializer[RowData] = _
      @transient var objectReuse = false
      lazy val map = new util.HashMap[String, Any]()

      override def onInit(parameters: Configuration): Unit = {
        super.onInit(parameters)
        objectReuse = getRuntimeContext.getExecutionConfig.isObjectReuseEnabled
        if (objectReuse) {
          serializer = typeInformation.createSerializer(getRuntimeContext.getExecutionConfig)
        }
      }

      override def valueTransform(data: RowData): RowData = {
        if (objectReuse) serializer.copy(data) else data
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

      def data2EsRecord(row: RowData): util.Map[_, _] = {
        map.clear()
        for ((i, name, fieldGetter) <- fieldGetters) {
          if (!row.isNullAt(i)) {
            map.put(name, fieldGetter(row, i))
          } else if (!ignoreNullFields) {
            map.put(name, null)
          }

        }
        println(map)
        map
      }
    }
  }

  def getUpdateScriptCfg(allCols: Seq[String], cfg: Map[String, String], updateScriptorderBy: Seq[(String, Boolean)]): Map[String, String] = {
    assert(List("update", "upsert").contains(cfg(ES_WRITE_OPERATION)), "配置updateScriptorderBy必须在update,upsert模式下")
    assert(!cfg.contains(ES_UPDATE_SCRIPT_INLINE), s"不能同时配置updateScriptorderBy和$ES_UPDATE_SCRIPT_INLINE")
    val cols = allCols.filter(_ != cfg.getOrElse(ES_MAPPING_ID, ""))
    assert(updateScriptorderBy.map(_._1).forall(cols.contains(_)), "updateScriptorderBy列不存在")
    val condition = updateScriptorderBy.map { case (col, ascending) =>
      if (ascending) {
        s"ctx._source.$col == null || ctx._source.$col < params.$col"
      } else {
        s"ctx._source.$col == null || ctx._source.$col > params.$col"
      }
    }.mkString(" || ")
    val update = cols.map(col => s"ctx._source.$col = params.$col;").mkString(" ")
    val params = cols.map(col => s"$col:$col").mkString(",")
    val script = s"if($condition){$update}"

    logWarning("gene script:" + script)
    logWarning("gene params:" + params)

    Map(
      ES_UPDATE_SCRIPT_INLINE -> script,
      ES_UPDATE_SCRIPT_PARAMS -> params,
      ES_UPDATE_SCRIPT_LANG -> "painless"
    )
  }

  val ignoreNullFields = false
  type ValueGetter = (RowData, Int) => Any
  type ArrayValueGetter = (ArrayData, Int) => Any

  def makeGetter(logicalType: LogicalType): ValueGetter = logicalType.getTypeRoot match {
    case CHAR | VARCHAR => (row, i) => row.getString(i).toString
    case INTEGER => (row, i) => row.getInt(i)
    case BIGINT => (row, i) => row.getLong(i)
    case FLOAT => (row, i) => row.getFloat(i)
    case DOUBLE => (row, i) => row.getDouble(i)
    case ROW =>
      val fields = logicalType.asInstanceOf[RowType].getFields.asScala
      val fieldGetters = fields.map(_.getType).map(makeGetter)
      val names = fields.map(_.getName)
      (_row, _i) => {
        val row = _row.getRow(_i, names.length)
        val map = new util.HashMap[String, Any](names.length)
        var i = 0
        while (i < names.size) {
          val name = names(i)
          if (!row.isNullAt(i)) {
            map.put(name, fieldGetters(i)(row, i))
          } else if (!ignoreNullFields) {
            map.put(name, null)
          }
          i += 1
        }
        map
      }
    case ARRAY =>
      val elementGetter = makeArrayGetter(logicalType.asInstanceOf[ArrayType].getElementType)
      (row, _i) => {
        val array = row.getArray(_i)
        val datas = new Array[Any](array.size())
        var i = 0
        while (i < array.size()) {
          if (!array.isNullAt(i)) {
            datas(i) = elementGetter(array, i)
          }
          i += 1
        }
        datas
      }
    case MAP =>
      val keyFieldGetter = makeArrayGetter(logicalType.asInstanceOf[MapType].getKeyType)
      val valueFieldGetter = makeArrayGetter(logicalType.asInstanceOf[MapType].getValueType)
      (row, _i) => {
        val map = row.getMap(_i)
        val keyArray = map.keyArray()
        val valueArray = map.valueArray()
        val obj = new util.HashMap[String, Any](keyArray.size())

        var i = 0
        while (i < map.size()) {
          if (!valueArray.isNullAt(i)) {
            obj.put(keyFieldGetter(keyArray, i).toString, valueFieldGetter(valueArray, i))
          } else {
            obj.put(keyFieldGetter(keyArray, i).toString, null)
          }
          i += 1
        }

        obj
      }
    case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
  }

  def makeArrayGetter(logicalType: LogicalType): ArrayValueGetter = logicalType.getTypeRoot match {
    case CHAR | VARCHAR => (array, i) => array.getString(i).toString
    case INTEGER => (array, i) => array.getInt(i)
    case BIGINT => (array, i) => array.getLong(i)
    case FLOAT => (array, i) => array.getFloat(i)
    case DOUBLE => (array, i) => array.getDouble(i)
    case ROW =>
      val fields = logicalType.asInstanceOf[RowType].getFields.asScala
      val fieldGetters = fields.map(_.getType).map(makeGetter)
      val names = fields.map(_.getName)
      (array, _i) => {
        val row = array.getRow(_i, names.length)
        val map = new util.HashMap[String, Any](names.length)
        var i = 0
        while (i < names.size) {
          val name = names(i)
          if (!row.isNullAt(i)) {
            map.put(name, fieldGetters(i)(row, i))
          } else if (!ignoreNullFields) {
            map.put(name, null)
          }
          i += 1
        }
        map
      }
    case ARRAY =>
      val elementGetter = makeArrayGetter(logicalType.asInstanceOf[ArrayType].getElementType)
      (_array, _i) => {
        val array = _array.getArray(_i)
        val datas = new Array[Any](array.size())
        var i = 0
        while (i < array.size()) {
          if (!array.isNullAt(i)) {
            datas(i) = elementGetter(array, i)
          }
          i += 1
        }
        datas
      }
    case MAP =>
      val keyFieldGetter = makeArrayGetter(logicalType.asInstanceOf[MapType].getKeyType)
      val valueFieldGetter = makeArrayGetter(logicalType.asInstanceOf[MapType].getValueType)
      (array, _i) => {
        val map = array.getMap(_i)
        val keyArray = map.keyArray()
        val valueArray = map.valueArray()
        val obj = new util.HashMap[String, Any](keyArray.size())

        var i = 0
        while (i < map.size()) {
          if (!valueArray.isNullAt(i)) {
            obj.put(keyFieldGetter(keyArray, i).toString, valueFieldGetter(valueArray, i))
          } else {
            obj.put(keyFieldGetter(keyArray, i).toString, null)
          }
          i += 1
        }

        obj
      }
    case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
  }


}
