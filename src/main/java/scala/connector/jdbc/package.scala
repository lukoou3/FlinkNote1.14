package scala.connector

import java.sql.PreparedStatement

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, CHAR, DOUBLE, FLOAT, INTEGER, VARCHAR}

import scala.collection.JavaConverters._

package object jdbc {

  //JdbcSinkOptions
  //JdbcConnectionOptions
  case class JdbcConnectionOptions(
    url: String,
    username: String,
    password: String,
    driverName: String
  )

  trait PeriodExecSqlStrategy extends Serializable {
    def sqlsThisTime(ts: Long): List[String]
  }

  implicit class ProductDataStreamJdbcFunctions[T <: Product : TypeInformation](ds: DataStream[T]) {
    def addBatchIntervalJdbcSink(
      tableName: String,
      connectionOptions: JdbcConnectionOptions,
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      maxRetries: Int = 2,
      fieldsForTuple: Seq[String] = Nil,
      isUpdateMode: Boolean = true,
      oldValcols: Seq[String] = Nil,
      fieldColMap: Map[String, String] = Map.empty[String, String],
      objectReuse: Boolean = false
    ): DataStreamSink[T] = {
      // 提醒作用, 调用者显示设置objectReuse=true则认为他知道objectReuse的影响
      assert(ds.executionEnvironment.getConfig.isObjectReuseEnabled == objectReuse)
      val productTypeInformation = implicitly[TypeInformation[T]].asInstanceOf[CaseClassTypeInfo[T]]
      val fieldNames = if (fieldsForTuple.isEmpty) {
        productTypeInformation.fieldNames
      } else {
        assert(fieldsForTuple.length == productTypeInformation.fieldNames.length)
        fieldsForTuple
      }
      val cols = fieldNames.map(x => fieldColMap.getOrElse(x, x))
      val names = fieldNames //fieldNames.zipWithIndex

      val sql = geneFlinkJdbcSql(tableName, cols, oldValcols, isUpdateMode)

      ds.addSink(new BatchIntervalJdbcSink[T](connectionOptions, batchSize = batchSize, batchIntervalMs = batchIntervalMs,
        minPauseBetweenFlushMs = minPauseBetweenFlushMs, maxRetries = maxRetries) {
        val numFields: Int = names.length

        override def updateSql: String = sql

        override def setStmt(stmt: PreparedStatement, data: T): Unit = {
          var i = 0
          while (i < numFields) {
            val v: AnyRef = data.productElement(i) match {
              case null => null
              case x: java.lang.Integer => x
              case x: java.lang.Long => x
              case x: java.sql.Timestamp => x
              case x: String => x
              case x => throw new UnsupportedOperationException(s"unsupported data $x")
            }
            stmt.setObject(i + 1, v)
            i = i + 1
          }
        }

      })
    }
  }

  implicit class DataStreamJdbcFunctions[T : TypeInformation](ds: DataStream[T]){
    def addKeyedBatchIntervalJdbcSink[K, E <: Product : TypeInformation](keyFunc: T => K)(dateFunc: T => E)(
      tableName: String,
      connectionOptions: JdbcConnectionOptions,
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      replaceDaTaValue: (T, T) => T = (newValue:T, oldValue:T) => newValue,
      maxRetries: Int = 2,
      fieldsForTuple: Seq[String] = Nil,
      isUpdateMode: Boolean = true,
      oldValcols: Seq[String] = Nil,
      fieldColMap: Map[String, String] = Map.empty[String, String],
      objectReuse: Boolean = false
    ): DataStreamSink[T] = {
      // 提醒作用, 调用者显示设置objectReuse=true则认为他知道objectReuse的影响
      assert(ds.executionEnvironment.getConfig.isObjectReuseEnabled == objectReuse)
      val productTypeInformation = implicitly[TypeInformation[E]].asInstanceOf[CaseClassTypeInfo[E]]
      val fieldNames = if (fieldsForTuple.isEmpty) {
        productTypeInformation.fieldNames
      } else {
        assert(fieldsForTuple.length == productTypeInformation.fieldNames.length)
        fieldsForTuple
      }
      val cols = fieldNames.map(x => fieldColMap.getOrElse(x, x))
      val names = fieldNames //fieldNames.zipWithIndex

      val sql = geneFlinkJdbcSql(tableName, cols, oldValcols, isUpdateMode)

      ds.addSink(new BatchIntervalJdbcSink[T](connectionOptions, batchSize = batchSize, batchIntervalMs = batchIntervalMs,
        minPauseBetweenFlushMs = minPauseBetweenFlushMs, keyedMode = true, maxRetries = maxRetries) {
        val numFields: Int = names.length

        override def updateSql: String = sql

        override def getKey(data: T): Any = keyFunc(data)

        override def replaceValue(newValue: T, oldValue: T): T = replaceDaTaValue(newValue, oldValue)

        override def setStmt(stmt: PreparedStatement, row: T): Unit = {
          val data = dateFunc(row)
          var i = 0
          while (i < numFields) {
            val v: AnyRef = data.productElement(i) match {
              case null => null
              case x: java.lang.Integer => x
              case x: java.lang.Long => x
              case x: java.sql.Timestamp => x
              case x => x.toString
            }
            stmt.setObject(i + 1, v)
            i = i + 1
          }
        }

      })

    }
  }

  implicit class TableFunctions(table: Table){
    def addRowDataBatchIntervalJdbcSink(
      tableName: String,
      connectionOptions: JdbcConnectionOptions,
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      keyedMode: Boolean = false,
      maxRetries: Int = 2,
      isUpdateMode: Boolean = true,
      oldValcols: Seq[String] = Nil,
      periodExecSqlStrategy: PeriodExecSqlStrategy = null
    ): DataStreamSink[RowData] = {
      val sink = getRowDataBatchIntervalJdbcSink(table.getResolvedSchema, tableName, connectionOptions, batchSize, batchIntervalMs,
        minPauseBetweenFlushMs =minPauseBetweenFlushMs, keyedMode=keyedMode, maxRetries=maxRetries, isUpdateMode=isUpdateMode,
        oldValcols= oldValcols, periodExecSqlStrategy=periodExecSqlStrategy)
      val rowDataDs = table.toDataStream[RowData](table.getResolvedSchema.toSourceRowDataType.bridgedTo(classOf[RowData]))
      rowDataDs.addSink(sink)
    }
  }

  def getRowDataBatchIntervalJdbcSink(
    resolvedSchema: ResolvedSchema,
    tableName: String,
    connectionOptions: JdbcConnectionOptions,
    batchSize: Int,
    batchIntervalMs: Long,
    minPauseBetweenFlushMs: Long = 100L,
    keyedMode: Boolean = false,
    maxRetries: Int = 2,
    isUpdateMode: Boolean = true,
    oldValcols: Seq[String] = Nil,
    periodExecSqlStrategy: PeriodExecSqlStrategy = null
  ): BatchIntervalJdbcSink[RowData] = {
    val typeInformation: InternalTypeInfo[RowData] = InternalTypeInfo.of(resolvedSchema.toSourceRowDataType.getLogicalType)
    val fieldInfos = resolvedSchema.getColumns.asScala.map(col => (col.getName, col.getDataType)).toArray
    val cols = fieldInfos.map(_._1)
    val sql = geneFlinkJdbcSql(tableName, cols, oldValcols, isUpdateMode)
    val _setters = fieldInfos.map{ case (_, dataType) =>
      val func: (PreparedStatement, RowData, Int) => Unit = dataType.getLogicalType.getTypeRoot match {
        case CHAR | VARCHAR => (stmt, row, i) =>
          stmt.setString(i+1, row.getString(i).toString)
        case INTEGER => (stmt, row, i) =>
          stmt.setInt(i+1, row.getInt(i))
        case BIGINT => (stmt, row, i) =>
          stmt.setLong(i+1, row.getLong(i))
        case FLOAT => (stmt, row, i) =>
          stmt.setFloat(i+1, row.getFloat(i))
        case DOUBLE => (stmt, row, i) =>
          stmt.setDouble(i+1, row.getDouble(i))
        case _ => throw new UnsupportedOperationException(s"unsupported data type $dataType")
      }
      func
    }

    new  BatchIntervalJdbcSink[RowData](connectionOptions,batchSize,batchIntervalMs,minPauseBetweenFlushMs,
      keyedMode ,maxRetries, periodExecSqlStrategy){
      val setters = _setters
      val numFields = setters.length
      @transient var serializer: TypeSerializer[RowData] = _
      @transient var objectReuse = false

      def updateSql: String = sql

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

      def setStmt(stmt: PreparedStatement, row: RowData): Unit = {
        var i = 0
        while (i < numFields) {
          if(row.isNullAt(i)){
            stmt.setObject(i + 1, null)
          }else{
            setters(i).apply(stmt, row, i)
          }
          i += 1
        }
      }
    }
  }


  private[jdbc] def geneFlinkJdbcSql(tableName: String, cols: Seq[String], oldValcols: Seq[String], isUpdateMode: Boolean): String = {
    val columns = cols.mkString(",")
    val placeholders = cols.map(_ => "?").mkString(",")
    val sql = if (isUpdateMode) {
      val duplicateSetting = cols.map { name =>
        if (oldValcols.contains(name)) {
          s"${name}=IFNULL(${name},VALUES(${name}))"
        } else {
          s"${name}=VALUES(${name})"
        }
      }.mkString(",")

      s"INSERT INTO $tableName ($columns) VALUES ($placeholders) ON DUPLICATE KEY UPDATE $duplicateSetting"
    } else {
      s"INSERT INTO $tableName ($columns) VALUES ($placeholders)"
    }
    sql
  }
}
