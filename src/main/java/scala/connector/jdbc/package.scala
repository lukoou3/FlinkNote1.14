package scala.connector

import java.sql.PreparedStatement

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.data.RowData

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

  implicit class ProductDataStreamJdbFunctions[T <: Product : TypeInformation](ds: DataStream[T]) {
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

      ds.addSink(new BatchIntervalJdbcSink[T](sql, connectionOptions, batchSize = batchSize, batchIntervalMs = batchIntervalMs,
        minPauseBetweenFlushMs = minPauseBetweenFlushMs, maxRetries = maxRetries) {
        val numFields: Int = names.length
        override def setStmt(stmt: PreparedStatement, data: T): Unit = {
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

      ds.addSink(new BatchIntervalJdbcSink[T](sql, connectionOptions, batchSize = batchSize, batchIntervalMs = batchIntervalMs,
        minPauseBetweenFlushMs = minPauseBetweenFlushMs, keyedMode = true, maxRetries = maxRetries) {
        val numFields: Int = names.length

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

  abstract class RowDataBatchIntervalJdbcSink(
    sql: String,
    connectionOptions: JdbcConnectionOptions,
    batchSize: Int,
    batchIntervalMs: Long,
    minPauseBetweenFlushMs: Long = 100L,
    keyedMode: Boolean = false,
    maxRetries: Int = 2,
    periodExecSqlStrategy: PeriodExecSqlStrategy = null
  ) extends BatchIntervalJdbcSink[RowData](sql,connectionOptions,batchSize,batchIntervalMs,minPauseBetweenFlushMs,keyedMode,maxRetries,periodExecSqlStrategy){


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
