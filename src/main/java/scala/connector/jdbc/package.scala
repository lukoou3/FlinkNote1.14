package scala.connector

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

}
