package scala.connector.test

import com.alibaba.fastjson.JSON
import org.scalatest.funsuite.AnyFunSuite

import java.util

import scala.connector.es.EsRestClient

class MyEsRestClientSuite extends AnyFunSuite{
 val client = new EsRestClient("127.0.0.1:9200", None, None)

  /**
   * 根据id查询：http://127.0.0.1:9200/poetry_test/poem_test/12
   * 根据id查询：http://127.0.0.1:9200/index_test30/type_test/12
   * 查询全部：http://127.0.0.1:9200/poetry_test/poem_test/_search
   */
  test("ReadId"){
    val rst = client.get("index_test30/type_test/12"){ case (code, rst) =>
      println("code", code)
      rst
    }
    val obj = JSON.parseObject(rst, classOf[util.Map[_,_]])
    println(obj.getClass)
    println(obj)
    println(JSON.toJSONString(obj,true))
  }

  test("_search"){
    val rst = client.get("index_test30/type_test/_search"){ case (code, rst) =>
      println("code", code)
      rst
    }
    val obj = JSON.parseObject(rst, classOf[util.Map[_,_]])
    println(obj)
    println(JSON.toJSONString(obj,true))
  }

  test("_delete_by_query"){
    val query = """{"query": {"bool": {"must": [{"range": {"year": {"lt": 18}}}]}}}"""
    val rst = client.post("index_test30/type_test/_delete_by_query", body = query){ case (code, rst) =>
      println("code", code)
      rst
    }
    val obj = JSON.parseObject(rst, classOf[util.Map[_,_]])
    println(obj)
    println(JSON.toJSONString(obj,true))
  }

  test("_delete_by_query2"){
    val query = """{"query": {"bool": {"must": [{"range": {"title": {"lt": "|"}}}]}}}"""
    val rst = client.post("index_test2/type_test/_delete_by_query", query){ case (code, rst) =>
      println("code", code)
      rst
    }
    val obj = JSON.parseObject(rst, classOf[util.Map[_,_]])
    println(obj)
    println(JSON.toJSONString(obj,true))
  }

}
