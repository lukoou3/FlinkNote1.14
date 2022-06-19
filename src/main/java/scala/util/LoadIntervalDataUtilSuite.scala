package scala.util

import org.scalatest.funsuite.AnyFunSuite

class LoadIntervalDataUtilSuite extends AnyFunSuite{

  test("LoadIntervalDataUtil"){
    val util: LoadIntervalDataUtil[Long] = LoadIntervalDataUtil(intervalMs = 3000) {
      System.currentTimeMillis()
    }

    println(util.data)
    for(_ <- 1 to 10){
      Thread.sleep(1000)
      println(util.data)
    }

    util.stop()

  }

}
