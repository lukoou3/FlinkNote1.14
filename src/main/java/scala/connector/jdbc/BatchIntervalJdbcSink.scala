package scala.connector.jdbc

import java.io.IOException
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import org.apache.flink.configuration.Configuration

import scala.collection.Iterable
import scala.connector.common.BatchIntervalSink
import scala.log.Logging

abstract class BatchIntervalJdbcSink[T](
  connectionOptions: JdbcConnectionOptions,
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 100L,
  keyedMode: Boolean = false,
  maxRetries: Int = 2,
  periodExecSqlStrategy: PeriodExecSqlStrategy = null
) extends BatchIntervalSink[T](batchSize, batchIntervalMs, minPauseBetweenFlushMs, keyedMode) with Logging {
  @transient var conn: Connection = _
  @transient var stmt: PreparedStatement = _

  def updateSql: String

  def setStmt(stmt: PreparedStatement, data: T): Unit

  override def onInit(parameters: Configuration): Unit = {
    initConn()
    stmt = conn.prepareStatement(updateSql)
  }

  final def initConn(): Unit = {
    val JdbcConnectionOptions(url, username, password, driverName) = connectionOptions
    Class.forName(driverName)
    conn = DriverManager.getConnection(url, username, password)
    conn.setAutoCommit(true)
    logInfo("open conn")
    logInfo(s"sql:$updateSql")
  }

  override final def onFlush(datas: Iterable[T]): Unit = {
    if (periodExecSqlStrategy != null) {
      periodExecSql()
    }
    logDebug("onFlush start")

    var i = 0
    while (i <= maxRetries) {
      logDebug("Retriy num:" + i)
      try {
        // 成功直接返回
        saveDatas(datas)
        return
      } catch {
        case e: SQLException =>
          if (i >= maxRetries) {
            throw new IOException(e)
          }

          // 连接超时, 重新连接
          try {
            if (!conn.isValid(60)) {
              initConn()
              if (stmt != null) {
                stmt.close()
              }
              stmt = conn.prepareStatement(updateSql)
            }
          } catch {
            case e: Exception =>
              throw new IOException("Reestablish JDBC connection failed", e)
          }
      }

      i += 1
    }
  }

  final def saveDatas(datas: Iterable[T]): Unit = {
    for (data <- datas) {
      setStmt(stmt, data)
      stmt.addBatch()
    }
    stmt.executeBatch()
  }

  final def periodExecSql(): Unit = {
    val ts = System.currentTimeMillis()
    val sqls = periodExecSqlStrategy.sqlsThisTime(ts)
    if (sqls.nonEmpty) {
      try {
        for (sql <- sqls) {
          val rst = stmt.executeUpdate(sql)
          logInfo(s"executeUpdate sql:$sql, rst:$rst")
        }
      } catch {
        case e: Exception =>
          logError("periodExecSql error", e)
      }
    }
  }

  override final def onClose(): Unit = {
    if (stmt != null) {
      logInfo("close stmt")
      stmt.close()
    }
    if (conn != null) {
      logInfo("close conn")
      conn.close()
    }
  }
}
