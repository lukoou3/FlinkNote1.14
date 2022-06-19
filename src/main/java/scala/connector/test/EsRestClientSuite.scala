package scala.connector.test

import java.util

import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.RestClient
import org.scalatest.funsuite.AnyFunSuite

class EsRestClientSuite extends AnyFunSuite {

  /**
   * 根据id查询：http://127.0.0.1:9200/poetry_test/poem_test/12
   * 根据id查询：http://127.0.0.1:9200/index_test30/type_test/12
   * 查询全部：http://127.0.0.1:9200/poetry_test/poem_test/_search
   */
  test("testReadId"){
    val settings = new PropertiesSettings()
    settings.setProperty("es.nodes", "127.0.0.1:9200")
    settings.setProperty("es.resource",  "index_test30/type_test")
    val client = new RestClient(settings)
    val rst = client.get[util.Map[String, AnyRef]]("index_test30/type_test/12", null)
    println(rst)
  }

  test("_mapping"){
    val settings = new PropertiesSettings()
    val client = new RestClient(settings)
    val rst = client.get[util.Map[String, AnyRef]]("index_test30/_mapping/type_test", null)
    println(rst)
  }

}
