package scala.sql.json

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.flink.table.data.StringData

object CreateJacksonParser {

  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }

  def utf8String(jsonFactory: JsonFactory, record: StringData): JsonParser = {
    val bb = record.toBytes

    val bain = new ByteArrayInputStream(bb)

    jsonFactory.createParser(new InputStreamReader(bain, StandardCharsets.UTF_8))
  }

}
