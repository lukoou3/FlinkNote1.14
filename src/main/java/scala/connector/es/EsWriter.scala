package scala.connector.es

import java.util.Properties

import org.apache.commons.logging.{Log, LogFactory}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.{PropertiesSettings, Settings}
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider
import org.elasticsearch.hadoop.rest.{InitializationUtils, RestService}
import org.elasticsearch.hadoop.security.UserProvider
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.hadoop.serialization.{BytesConverter, JdkBytesConverter}
import org.elasticsearch.spark.serialization.{ScalaMapFieldExtractor, ScalaValueWriter}

import scala.collection.JavaConverters._

/**
 *
 * https://www.elastic.co/guide/en/elasticsearch/hadoop/8.4/spark.html
 */
class EsWriter[T](val cfg: Map[String, String]) extends Serializable {
  @transient protected lazy val log: Log = LogFactory.getLog(this.getClass)

  @transient lazy val settings: Settings = {
    val props = new Properties
    val defaultCfg = Map(
      ES_MAPPING_DATE_RICH_OBJECT -> "false",
      ES_INDEX_AUTO_CREATE -> "false"
    ) ++ (if(cfg.getOrElse(ES_INPUT_JSON, "false") != "true") Map(ES_MAPPING_EXCLUDE -> "_id") else  Map.empty)
    val rstCfg = defaultCfg ++ cfg
    props.putAll(rstCfg.asJava)

    val settings = new PropertiesSettings(props)

    // 处理输入的scala类型, 比如map
    InitializationUtils.setValueWriterIfNotSet(settings, valueWriter, log)
    InitializationUtils.setBytesConverterIfNeeded(settings, bytesConverter, log)
    InitializationUtils.setFieldExtractorIfNotSet(settings, fieldExtractor, log)
    //InitializationUtils.setMetadataExtractorIfNotSet(settings, metadataExtractor, log)
    InitializationUtils.setUserProviderIfNotSet(settings, userProvider, log)

    settings
  }

  @transient var writer: RestService.PartitionWriter = null

  def init(): Unit = {
    checkEsSettings(settings)
    writer = RestService.createWriter(settings, 0, -1, log)
  }

  def close(): Unit = {
    if (writer != null) {
      writer.close()
    }
  }

  def flush(): Unit = {
    writer.repository.flush()
  }

  def write(data: T): Unit = {
    writer.repository.writeToIndex(processData(data))
  }

  def client = writer.repository.getRestClient

  def repository = writer.repository

  protected def processData(data: T): Any = {
    data
  }

  protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[ScalaValueWriter]

  protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]

  protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[ScalaMapFieldExtractor]

  //protected def metadataExtractor: Class[_ <: MetadataExtractor] = classOf[ScalaMetadataExtractor]
  protected def userProvider: Class[_ <: UserProvider] = classOf[HadoopUserProvider]

  private def checkEsSettings(config: Settings): Unit = {
    //assert(!settings.getIndexAutoCreate(), "cant not auto create index")

    // Need to discover the EsVersion here before checking if the index exists
    InitializationUtils.setUserProviderIfNotSet(config, classOf[HadoopUserProvider], log)
    InitializationUtils.discoverClusterInfo(config, log)
    InitializationUtils.checkIdForOperation(config)
    // 检查index是否存在
    InitializationUtils.checkIndexExistence(config)
  }
}
