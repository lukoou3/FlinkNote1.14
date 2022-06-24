package scala.connector.common

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.{RowData, StringData}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import scala.collection.JavaConverters._
import scala.math.Ordering

object Utils {

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
