package scala.connector.reflect

import java.util

import org.apache.flink.configuration.ConfigOption
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSourceFactory}

class ReflectTableFactory extends DynamicTableSourceFactory{

  def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {

    //
    ???
  }

  def factoryIdentifier(): String = "reflect"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions
  }
}

object ReflectTableFactory{

}
