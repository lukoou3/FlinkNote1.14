package scala.stream.sink

import java.sql.PreparedStatement

object JdbcSinks {

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

  trait JdbcSinkData

  // a A private
  class TableSave[T] (
    val sql: String,
    val setStmt: (PreparedStatement, T) => Unit,
    val periodExecSqlStrategy: PeriodExecSqlStrategy = null
  ) extends Serializable

  object TableSave{

  }

}
