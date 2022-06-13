package scala.util

import org.scalatest.funsuite.AnyFunSuite

class UtilsSuite extends AnyFunSuite {

  test("getYAMLResourceConf"){
    val is = classOf[UtilsSuite].getClassLoader().getResourceAsStream("flink-conf.yaml")
    val map = Utils.getYAMLResourceConf(is)
    map.foreach(println(_))
  }

}
