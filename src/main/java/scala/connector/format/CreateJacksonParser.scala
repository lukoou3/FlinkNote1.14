package scala.connector.format

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.flink.table.data.StringData

object CreateJacksonParser {

  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }

  def utf8String(jsonFactory: JsonFactory, record: StringData): JsonParser = {
    val bb = record.toBytes

    //val bain = new ByteArrayInputStream(bb)
    //jsonFactory.createParser(new InputStreamReader(bain, StandardCharsets.UTF_8))

    jsonFactory.createParser(bb, 0, bb.length)
  }

  def bytes(jsonFactory: JsonFactory, bytes: Array[Byte]): JsonParser = {
    jsonFactory.createParser(bytes, 0, bytes.length)
  }

}
