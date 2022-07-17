package scala.connector.common

import javax.annotation.Nullable
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.{Table, ValidationException}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.internal.TableImpl
import org.apache.flink.table.catalog.SchemaTranslator.ProducingResult
import org.apache.flink.table.catalog.{ResolvedSchema, SchemaTranslator}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._
import scala.math.Ordering

object Utils {

  def getRowDataDataStreamInternal(table: Table, @Nullable changelogMode: ChangelogMode): DataStream[RowData] ={
    val internalEnv = table.asInstanceOf[TableImpl].getTableEnvironment
    internalEnv match {
      case tableEnv: StreamTableEnvironment =>
        val rowDataDataType: DataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
        Preconditions.checkNotNull(table, "Table must not be null.")
        Preconditions.checkNotNull(rowDataDataType, "Target data type must not be null.")

        val tableEnvImpl = tableEnv.asInstanceOf[StreamTableEnvironmentImpl]

        val schemaTranslationResult = SchemaTranslator.createProducingResult(
          tableEnvImpl.getCatalogManager.getDataTypeFactory,
          table.getResolvedSchema,
          rowDataDataType)

        val toStreamInternal = classOf[StreamTableEnvironmentImpl].getDeclaredMethod("toStreamInternal",
          classOf[Table], classOf[ProducingResult], classOf[ChangelogMode])
        toStreamInternal.setAccessible(true)

        toStreamInternal.invoke(tableEnvImpl, table, schemaTranslationResult, changelogMode).asInstanceOf[DataStream[RowData]]
      case _ =>
        throw new ValidationException(
          "Table cannot be converted into a Scala DataStream. " +
            "It is not part of a Scala StreamTableEnvironment.")
    }
  }

  implicit class StringCfgOps(str:String){

    def toSinkKeyedModeKeys: Seq[String] = str.split(",").map(_.trim).filter(_.nonEmpty)

    def toSinkKeyedModeOrderBy: Seq[(String, Boolean)] = str.split(",").flatMap{ text =>
      val kv = text.trim.split("""\s+""")
      if(kv.length >= 1){
        val ascending = if(kv.length < 2){
          true
        } else{
          kv(1).trim.toLowerCase match {
            case "asc" => true
            case "desc" => false
            case _ => throw new Exception("ascending的值必须是asc或者desc")
          }
        }
        Some((kv(0).trim, ascending))
      }else{
        None
      }
    }

  }

  def getTableKeyFunction(resolvedSchema: ResolvedSchema, keyedMode: Boolean, keys: Seq[String]): RowData => Any ={
    if(keyedMode){
      assert(keys.nonEmpty, "keyedMode下keys不能为空")
    }
    val colMap = resolvedSchema.getColumns.asScala.zipWithIndex.map { case (col, i) => (col.getName, (col, i)) }.toMap
    val keyGetters = keys.map { colName =>
      val (col, i) = colMap.getOrElse(colName, throw new Exception("不存在的列:" + colName))
      val fieldGetter = RowData.createFieldGetter(col.getDataType.getLogicalType, i)
      fieldGetter
    }.toArray
    if (keyGetters.length == 0) {
      null
    } else if (keyGetters.length == 1) {
      row => keyGetters(0).getFieldOrNull(row)
    } else {
      row => GenericRowData.of(keyGetters.map(_.getFieldOrNull(row)): _*)
    }
  }

  def getTableOrdering(resolvedSchema: ResolvedSchema, orderBy: Seq[(String, Boolean)]):Ordering[RowData] = {
    val colMap = resolvedSchema.getColumns.asScala.zipWithIndex.map{case(col, i) => (col.getName, (col, i))}.toMap
    val fieldGetterAndOrderings = orderBy.map{ case (colName, ascending) =>
      val (col, i) = colMap.getOrElse(colName, throw new Exception("不存在的列:" + colName))
      val fieldGetter = RowData.createFieldGetter(col.getDataType.getLogicalType, i)
      val ordering = if(ascending) {
        wrapNullableOrdering(getOrdering(col.getDataType.getLogicalType))
      }else{
        wrapNullableOrdering(getOrdering(col.getDataType.getLogicalType)).reverse
      }
      (fieldGetter, ordering)
    }
    new Ordering[RowData] {
      override def compare(x: RowData, y: RowData): Int = {
        var i = 0
        val size = fieldGetterAndOrderings.size
        while (i < size) {
          val fieldGetter = fieldGetterAndOrderings(i)._1
          val ordering = fieldGetterAndOrderings(i)._2
          val left = fieldGetter.getFieldOrNull(x)
          val right = fieldGetter.getFieldOrNull(y)

          val comparison = ordering.compare(left, right)
          if (comparison != 0) {
            return comparison
          }

          i += 1
        }

        0
      }
    }
  }

  def wrapNullableOrdering[T](ordering: Ordering[T]): Ordering[T]= {
    new Ordering[T]{
      override def compare(x: T, y: T): Int = {
        if(x == null && y == null){
          0
        }else if (x == null) {
          -1
        } else if (y == null) {
          1
        } else {
          ordering.compare(x, y)
        }
      }
    }
  }

  def getOrdering(logicalType: LogicalType):Ordering[Any] = {
    (logicalType.getTypeRoot match {
      case INTEGER => implicitly[Ordering[Int]]
      case BIGINT => implicitly[Ordering[Long]]
      case FLOAT => implicitly[Ordering[Float]]
      case DOUBLE => implicitly[Ordering[Double]]
      case CHAR | VARCHAR => new Ordering[StringData]{
        override def compare(x: StringData, y: StringData): Int = x.compareTo(y)
      }
      case ROW => new Ordering[RowData] {
        val orderings = logicalType.asInstanceOf[RowType].getFields.asScala.map{field =>
          getOrdering(field.getType)
        }.toArray
        val fieldGetters = logicalType.asInstanceOf[RowType].getFields.asScala.zipWithIndex.map{ case(field, i) =>
          RowData.createFieldGetter(field.getType, i)
        }.toArray
        override def compare(x: RowData, y: RowData): Int = {
          var i = 0
          val size = orderings.size
          while (i < size) {
            val order = orderings(i)
            val left = fieldGetters(i).getFieldOrNull(x)
            val right = fieldGetters(i).getFieldOrNull(y)

            if (left == null && right == null) {
              // Both null, continue looking.
            } else if (left == null) {
              return -1
            } else if (right == null) {
              return 1
            } else {
              val comparison = order.compare(left, right)
              if (comparison != 0) {
                return comparison
              }
            }

            i += 1
          }

          0
        }
      }
      case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
    }).asInstanceOf[Ordering[Any]]
  }
}
