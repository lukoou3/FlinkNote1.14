package scala.stream.sink

import java.io.BufferedWriter
import java.nio.file.{Files, Paths}
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * Transactional sink that writes records to files an commits them to a target directory.
 *
 * Records are written as they are received into a temporary file. For each checkpoint, there is
 * a dedicated file that is committed once the checkpoint (or a later checkpoint) completes.
 */
class TransactionalFileSink[T](val targetPath: String, val tempPath: String)
  extends TwoPhaseCommitSinkFunction[T, String, Void](
    createTypeInformation[String].createSerializer(new ExecutionConfig),
    createTypeInformation[Void].createSerializer(new ExecutionConfig)) {

  var transactionWriter: BufferedWriter = _
  lazy val fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  /**
   * Creates a temporary file for a transaction into which the records are written.
   */
  override def beginTransaction(): String = {

    // path of transaction file is constructed from current time and task index
    val timeNow = LocalDateTime.now().format(fmt)
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    val transactionFile = s"$timeNow-$taskIdx"

    // create transaction file and writer
    val tFilePath = Paths.get(s"$tempPath/$transactionFile")
    Files.createFile(tFilePath)
    this.transactionWriter = Files.newBufferedWriter(tFilePath)
    println(s"Creating Transaction File: $tFilePath")

    // name of transaction file is returned to later identify the transaction
    transactionFile
  }

  /** Write record into the current transaction file. */
  override def invoke(transaction: String, value: T, context: Context): Unit = {
    transactionWriter.write(value.toString)
    transactionWriter.write('\n')
  }

  /** Flush and close the current transaction file. */
  override def preCommit(transaction: String): Unit = {
    transactionWriter.flush()
    transactionWriter.close()
  }

  /** Commit a transaction by moving the pre-committed transaction file
   * to the target directory.
   */
  override def commit(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    // check if the file exists to ensure that the commit is idempotent.
    if (Files.exists(tFilePath)) {
      val cFilePath = Paths.get(s"$targetPath/$transaction")
      Files.move(tFilePath, cFilePath)
    }
  }

  /** Aborts a transaction by deleting the transaction file. */
  override def abort(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    if (Files.exists(tFilePath)) {
      Files.delete(tFilePath)
    }
  }
}
