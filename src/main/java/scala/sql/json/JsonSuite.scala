package scala.sql.json

import com.fasterxml.jackson.core.{JsonFactoryBuilder, JsonParser, JsonToken}
import com.fasterxml.jackson.core.json.JsonReadFeature
import org.apache.flink.table.data.StringData
import org.scalatest.funsuite.AnyFunSuite

class JsonSuite extends AnyFunSuite{

  test("test"){
    val jsonFactory = new JsonFactoryBuilder()
      // The two options below enabled for Hive compatibility
      .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
      .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
      .build()

    var jsonParser: JsonParser = null
    try {
      jsonParser = jsonFactory.createParser("""{"name":"实时", "age":20, "cnt":null}""")
      var m = 0
      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        val filedName = jsonParser.getCurrentName
        if (filedName == "name") {
          jsonParser.nextToken()
          val value = jsonParser.getText
          println(value)
          m = m + 1
        }else if (filedName == "age") {
          jsonParser.nextToken()
          val value = jsonParser.getText
          println(value)
          m = m + 1
        }else if (filedName == "cnt") {
          jsonParser.nextToken()
          val value = jsonParser.getText
          println(value)
          m = m + 1
        }

      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (jsonParser != null) {
        jsonParser.close()
      }
    }
  }

  test("test2"){
    val jsonFactory = new JsonFactoryBuilder()
      // The two options below enabled for Hive compatibility
      .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
      .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
      .build()

    var jsonParser: JsonParser = null
    try {
      jsonParser = jsonFactory.createParser("""{"name":"实时", "age":20, "cnt":null}""".getBytes("utf-8"))
      var m = 0
      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        val filedName = jsonParser.getCurrentName
        if (filedName == "name") {
          jsonParser.nextToken()
          val value = jsonParser.getText
          println(value)
          m = m + 1
        }else if (filedName == "age") {
          jsonParser.nextToken()
          val value = jsonParser.getText
          println(value)
          m = m + 1
        }else if (filedName == "cnt") {
          jsonParser.nextToken()
          val value = jsonParser.getText
          println(value)
          m = m + 1
        }

      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (jsonParser != null) {
        jsonParser.close()
      }
    }
  }

}
