package scala.stream.sink

import java.io.IOException
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import org.apache.flink.configuration.Configuration

import scala.log.Logging
import scala.stream.sink.JdbcSinks.{JdbcConnectionOptions, PeriodExecSqlStrategy}

abstract class BatchIntervalJdbcSink[T](
  sql: String,
  connectionOptions: JdbcConnectionOptions,
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 100L,
  maxRetries: Int = 2,
  periodExecSqlStrategy: PeriodExecSqlStrategy = null
) extends BatchIntervalSink[T](batchSize, batchIntervalMs, minPauseBetweenFlushMs) with Logging {
  @transient var conn: Connection = _
  @transient var stmt: PreparedStatement = _

  def setStmt(stmt: PreparedStatement, data: T): Unit

  override def onInit(parameters: Configuration): Unit = {
    initConn()
    stmt = conn.prepareStatement(sql)
  }

  final def initConn(): Unit = {
    val JdbcConnectionOptions(url, username, password, driverName) = connectionOptions
    Class.forName(driverName)
    conn = DriverManager.getConnection(url, username, password)
    conn.setAutoCommit(true)
    logInfo("open conn")
    logInfo(s"sql:$sql")
  }

  override final def onFlush(datas: Seq[T]): Unit = {
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
              stmt = conn.prepareStatement(sql)
            }
          } catch {
            case e: Exception =>
              throw new IOException("Reestablish JDBC connection failed", e)
          }
      }

      i += 1
    }
  }

  final def saveDatas(datas: Seq[T]): Unit = {
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
