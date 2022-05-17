package scala.util

import java.util.concurrent.TimeUnit

import scala.log.Logging

object ThreadUtilsTest extends Logging{

  def main(args: Array[String]): Unit = {
    test()
  }

  def test(): Unit ={
    val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeater-task")

    val heartbeatTask = new Runnable() {
      var i = 0
      override def run(): Unit =  {
        i += 1
        if(i % 4== 0){
          logWarning("error")
          throw new Exception("test")
        }else{
          logWarning(""+ i)
        }
      }
    }
    // scheduleAtFixedRate：固定速率调度，本身任务执行时长不影响
    // scheduleWithFixedDelay：固定延时调度，是在每个任务结束后延时一定时长执行任务
    heartbeater.scheduleAtFixedRate(heartbeatTask, 1, 5, TimeUnit.SECONDS)
    logWarning("sleep")

    Thread.sleep(1000 * 60 * 5)
  }

}
