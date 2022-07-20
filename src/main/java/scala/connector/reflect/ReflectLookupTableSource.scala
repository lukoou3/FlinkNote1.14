package scala.connector.reflect

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.source.{DynamicTableSource, LookupTableSource}

import scala.log.Logging

class ReflectLookupTableSource(
  path: String,
  physicalSchema: TableSchema
) extends LookupTableSource with Logging{
  def getLookupRuntimeProvider(context: LookupTableSource.LookupContext): LookupTableSource.LookupRuntimeProvider = {
    // ReflectRowDataLookupFunction
    ???
  }

  def copy(): DynamicTableSource = ???

  def asSummaryString(): String = "reflect"
}
