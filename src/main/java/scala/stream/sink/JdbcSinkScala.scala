package scala.stream.sink

import java.sql.PreparedStatement

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

object JdbcSinkScala {

  /**
   *
   * @param sql
   * @param f                scala11兼容java8，scala12的function支持java8的lambda
   * @param connectionOptions
   * @param executionOptions 默认的每次调用调用函数返回新的对象
   * @tparam T
   * @return
   */
  def sink[T](sql: String, f: (PreparedStatement, T) => Unit, connectionOptions: JdbcConnectionOptions,
              executionOptions: JdbcExecutionOptions): SinkFunction[T] = {
    JdbcSink.sink(sql, new JdbcStatementBuilder[T] {
      override def accept(t: PreparedStatement, u: T): Unit = f(t, u)
    }, executionOptions, connectionOptions)
  }

  def sink[T](sql: String, connectionOptions: JdbcConnectionOptions,
              executionOptions: JdbcExecutionOptions = JdbcExecutionOptions.defaults)(f: (PreparedStatement, T) => Unit): SinkFunction[T] = {
    JdbcSink.sink(sql, new JdbcStatementBuilder[T] {
      override def accept(t: PreparedStatement, u: T): Unit = f(t, u)
    }, executionOptions, connectionOptions)
  }
}
