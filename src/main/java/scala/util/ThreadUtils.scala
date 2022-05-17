package scala.util

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{AbstractExecutorService, ExecutorService, Executors, LinkedBlockingQueue, RejectedExecutionException, ScheduledExecutorService, ScheduledThreadPoolExecutor, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object ThreadUtils {
  private val sameThreadExecutionContext = ExecutionContext.fromExecutorService(sameThreadExecutorService())

  /**
   * Inspired by Guava MoreExecutors.sameThreadExecutor; inlined and converted to Scala here to avoid Guava version issues
   * 仿照Guava的MoreExecutors.sameThreadExecutor，在这用scala实现，避免Guava版本影响
   * @return
   */
  private def sameThreadExecutorService(): ExecutorService = new AbstractExecutorService {
    private val lock = new ReentrantLock()
    private val termination = lock.newCondition()
    private var runningTasks = 0
    private var serviceIsShutdown = false

    override def shutdown(): Unit = {
      lock.lock()
      try {
        serviceIsShutdown = true
      } finally {
        lock.unlock()
      }
    }

    override def shutdownNow(): java.util.List[Runnable] = {
      shutdown()
      java.util.Collections.emptyList()
    }

    override def isShutdown: Boolean = {
      lock.lock()
      try {
        serviceIsShutdown
      } finally {
        lock.unlock()
      }
    }

    override def isTerminated: Boolean = synchronized {
      lock.lock()
      try {
        serviceIsShutdown && runningTasks == 0
      } finally {
        lock.unlock()
      }
    }

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
      var nanos = unit.toNanos(timeout)
      lock.lock()
      try {
        while (nanos > 0 && !isTerminated()) {
          nanos = termination.awaitNanos(nanos)
        }
        isTerminated()
      } finally {
        lock.unlock()
      }
    }

    override def execute(command: Runnable): Unit = {
      lock.lock()
      try {
        if (isShutdown()) throw new RejectedExecutionException("Executor already shutdown")
        runningTasks += 1
      } finally {
        lock.unlock()
      }
      try {
        command.run()
      } finally {
        lock.lock()
        try {
          runningTasks -= 1
          if (isTerminated()) termination.signalAll()
        } finally {
          lock.unlock()
        }
      }
    }
  }

  /**
   * ExecutionContextExecutor：运行每个task在当前调用的线程，调用者必须保证运行的tasks必须很快执行完且不阻塞
   * An `ExecutionContextExecutor` that runs each task in the thread that invokes `execute/submit`.
   * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
   * never block.
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(
    prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }


  /**
   * 固定单个线程的 ExecutorService
   * Wrapper over newSingleThreadExecutor.
   */
  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

  /**
   * 包含单个线程的 ScheduledThreadPoolExecutor
   * Wrapper over ScheduledThreadPoolExecutor.
   */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }
}
