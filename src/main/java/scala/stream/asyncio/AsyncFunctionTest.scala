package scala.stream.asyncio

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.connector.localfile.LocalFileSourceFunction
import scala.util.ThreadUtils

/**
 * 使用和1.12一样
 * 依靠自己实现异步查询而不是别的api自带的功能，不用Executors.directExecutor()，这个运行在当前的线程，官方想让我们用这个实现回调函数的调用
 * 自己实现异步查询Future的apply方法和onSuccess方法都依赖线程池，这样就需要多个线程，不然还是同步的
 * 要是某些数据库自身提供的异步查询的api，就用Executors.directExecutor()
 * 使用异步查询，超时会直接抛出异常并重启作业。 如果你想处理超时，可以重写 AsyncFunction#timeout 方法。
 * [[org.apache.flink.streaming.api.operators.async.AsyncWaitOperator]]
 */
object AsyncFunctionTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()

    val onlineLog = env.addSource(
      new LocalFileSourceFunction("D:\\IdeaWorkspace\\FlinkNote\\Flink12\\test_data\\online_log.json",
        sleep = 1000, deserializer = new SimpleStringSchema())
      //new LocalFileSourceFunction("D:\\\\ideaProjects\\FlinkNote\\Flink12\\test_data\\online_log.json", sleep = 100)
    ).map(_ => new Timestamp(System.currentTimeMillis()).toString)

    val rstDs = AsyncDataStream.unorderedWait(onlineLog, new AsyncDatabaseRequest(),
      10, TimeUnit.SECONDS, 100)

    rstDs.addSink{ item =>
      println(item)
    }

    env.execute("AsyncFunctionTest")
  }

  class AsyncDatabaseRequest extends RichAsyncFunction[String, (String, String)] {
    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    // 实现io等耗时操作，用于本身的客户端不支持异步的情况
    var threadPoolExecutor: ThreadPoolExecutor = _
    // 调用回调函数
    var executor: ExecutionContextExecutor = _
    lazy val directExecutor = ThreadUtils.sameThread

    override def open(parameters: Configuration): Unit = {
      threadPoolExecutor = ThreadUtils.newDaemonFixedThreadPool(4, "Async-Func-SubTask" + getRuntimeContext.getIndexOfThisSubtask )
      executor = ExecutionContext.fromExecutor(threadPoolExecutor)
    }

    override def asyncInvoke(input: String, resultFuture: ResultFuture[(String, String)]): Unit = {
      val future = Future {
        Thread.sleep(2000)
        input + ":rst"
      }(executor)

      // 显示的指定后面的隐式参数, 这里不能用future onSuccess
      future.onSuccess{
        case result: String =>
          // 这里的线程肯定是executor里的，不过是直接调用的，没有调度的开销
          println(fmt.format(new Date), Thread.currentThread.getId, Thread.currentThread.getName)
          resultFuture.complete(Iterable((input, result)))
      }(directExecutor)
    }

    override def close(): Unit = {
      if(threadPoolExecutor != null){
        threadPoolExecutor.shutdown()
      }
    }
  }
}
