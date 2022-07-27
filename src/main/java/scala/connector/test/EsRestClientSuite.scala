package scala.connector.test

import java.util

import com.alibaba.fastjson.JSON
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.RestClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class EsRestClientSuite extends AnyFunSuite with BeforeAndAfterAll {
  var client: RestClient = null

  override protected def beforeAll(): Unit = {
    val settings = new PropertiesSettings()
    settings.setProperty("es.nodes", "127.0.0.1:9200")
    settings.setProperty("es.resource",  "index_test30/type_test")
    client = new RestClient(settings)
  }

  /**
   * 根据id查询：http://127.0.0.1:9200/poetry_test/poem_test/12
   * 根据id查询：http://127.0.0.1:9200/index_test30/type_test/12
   * 查询全部：http://127.0.0.1:9200/poetry_test/poem_test/_search
   */
  test("ReadId"){
    val rst = client.get[util.Map[String, AnyRef]]("index_test30/type_test/12", null)
    println(JSON.toJSONString(rst,true))
    val rst2 = client.get[util.Map[String, AnyRef]]("/index_test30/type_test/12", null)
    println(JSON.toJSONString(rst2,true))
  }

  test("_search"){
    val rst = client.get[util.Map[String, AnyRef]]("index_test30/type_test/_search", null)
    println(JSON.toJSONString(rst,true))
    println("\n" + "*" * 50 + "\n")
    val rst2 = client.get[util.Map[String, AnyRef]]("/index_test30/type_test/_search", "hits")
    println(JSON.toJSONString(rst2,true))
  }

  test("_search"){

  }

  test("_mapping"){
    val rst = client.get[util.Map[String, AnyRef]]("index_test30/_mapping/type_test", null)
    println(JSON.toJSONString(rst,true))
  }

  override protected def afterAll(): Unit = {
    client.close()
  }
}
