package scala.stream.sink

import java.io.IOException
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import org.apache.flink.configuration.Configuration

import scala.collection.mutable.HashMap
import scala.log.Logging
import scala.stream.sink.JdbcSinks.{JdbcConnectionOptions, JdbcSinkData, TableSave}

class ManyTableBatchIntervalJdbcSink(
  //tableSaves: Seq[(Class[_], TableSave[_ <: JdbcSinkData])],
  tableSaves: Map[Class[_], TableSave[_ <: JdbcSinkData]],
  connectionOptions: JdbcConnectionOptions,
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 100L,
  maxRetries: Int = 2
) extends BatchIntervalSink[JdbcSinkData](batchSize, batchIntervalMs, minPauseBetweenFlushMs) with Logging  {
  @transient var conn: Connection = _
  val stmts: HashMap[Class[_], PreparedStatement] = new HashMap()
  val setStmts = tableSaves.mapValues(_.setStmt.asInstanceOf[(PreparedStatement, JdbcSinkData) => Unit]).map(x => x)
  val periodExecSqlStrategys = tableSaves.mapValues(_.periodExecSqlStrategy).filter(_._2 != null).toList

  override def onInit(parameters: Configuration): Unit = {
    initConn()
    prepareStmts()
  }

  final def initConn(): Unit = {
    val JdbcConnectionOptions(url, username, password, driverName) = connectionOptions
    Class.forName(driverName)
    conn = DriverManager.getConnection(url, username, password)
    conn.setAutoCommit(true)
    logInfo("open conn")
  }

  final def prepareStmts(): Unit = {
    for ((clazz, data) <- tableSaves) {
      val stmt = conn.prepareStatement(data.sql)
      stmts.put(clazz, stmt)
    }
  }

  override def onFlush(datas: Seq[JdbcSinkData]): Unit = {
    logDebug("onFlush start")

    for ((clazz, subDatas) <- datas.groupBy(_.getClass)) {
      flushOneTable(clazz, subDatas)
    }

    // 后执行，省的连接超时
    periodExecSqls()
  }

  final def flushOneTable(clazz: Class[_], datas: Seq[JdbcSinkData]): Unit = {
    var i = 0
    while (i <= maxRetries) {
      logDebug("Retriy num:" + i)
      try {
        // 成功直接返回
        saveDatas(clazz, datas)
        return
      } catch {
        case e: SQLException =>
          if (i >= maxRetries) {
            throw new IOException(e)
          }

          // 连接超时, 重新连接
          try {
            if (!conn.isValid(60)) {
              try {
                conn.close()
              }catch {
                case e: SQLException =>
                  logError("JDBC connection close failed.", e)
              }
              initConn()
              closeStats()
              prepareStmts()
            }
          } catch {
            case e: Exception =>
              throw new IOException("Reestablish JDBC connection failed", e)
          }
      }

      i += 1
    }
  }

  final def saveDatas(clazz: Class[_], datas: Seq[JdbcSinkData]): Unit = {
    val stmt = stmts(clazz)
    val setStmt = setStmts(clazz)
    for (data <- datas) {
      setStmt(stmt, data)
      stmt.addBatch()
    }
    stmt.executeBatch()
  }

  final def periodExecSqls(): Unit = {
    val ts = System.currentTimeMillis()
    for ((clazz, periodExecSqlStrategy) <- periodExecSqlStrategys) {
      val sqls = periodExecSqlStrategy.sqlsThisTime(ts)
      if (sqls.nonEmpty) {
        val stmt = stmts(clazz)
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
  }

  def closeStats(): Unit ={
    for ((clazz, stmt) <- stmts) {
      logInfo(s"close stmt for $clazz")
      stmt.close()
    }
    stmts.clear()
  }

  override def onClose(): Unit = {
    closeStats()

    if (conn != null) {
      logInfo("close conn")
      conn.close()
    }
  }
}
