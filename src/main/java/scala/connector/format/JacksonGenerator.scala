package scala.connector.format

import com.fasterxml.jackson.core.JsonFactory
import java.io.Writer
import org.apache.flink.table.data.{ArrayData, MapData, RowData}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{ArrayType, LogicalType, MapType, RowType}
import scala.collection.JavaConverters._

/**
 * org.apache.spark.sql.DataFrameReader#json
 *
 * `JackGenerator` can only be initialized with a `StructType`, a `MapType` or an `ArrayType`.
 * Once it is initialized with `StructType`, it can be used to write out a struct or an array of
 * struct. Once it is initialized with `MapType`, it can be used to write out a map or an array
 * of map. An exception will be thrown if trying to write out a struct if it is initialized with
 * a `MapType`, and vice verse.
 */
class JacksonGenerator(
  logicalType: LogicalType,
  writer: Writer
) {
  private type ValueWriter = (RowData, Int) => Unit
  private type ArrayValueWriter = (ArrayData, Int) => Unit

  // `JackGenerator` can only be initialized with a `StructType`, a `MapType` or a `ArrayType`.
  require(logicalType.isInstanceOf[RowType] || logicalType.isInstanceOf[ArrayType]
    || logicalType.isInstanceOf[MapType],
    s"JacksonGenerator only supports to be initialized with a StructType, " +
      s"MapType or ArrayType but got $logicalType")

  // `ValueWriter`s for all fields of the schema
  private lazy val rootFieldWriters: Seq[ValueWriter] = logicalType match {
    case rowType: RowType => rowType.getFields.asScala.map(_.getType).map(makeWriter)
    case _ => throw new UnsupportedOperationException(
      s"Initial type $logicalType must be a StructType")
  }

  private lazy val rootFieldNames: Seq[String] = logicalType match {
    case rowType: RowType => rowType.getFields.asScala.map(_.getName)
    case _ => throw new UnsupportedOperationException(
      s"Initial type $logicalType must be a StructType")
  }

  // `ValueWriter` for array data storing rows of the schema.
  private lazy val arrElementWriter: ArrayValueWriter = logicalType match {
    case at: ArrayType => makeArrayWriter(at.getElementType)
    case _ => throw new UnsupportedOperationException(
      s"Initial type $logicalType must be a ArrayType")
  }

  private lazy val mapKeyWriter: ArrayValueWriter = logicalType match {
    case mt: MapType => makeArrayWriter(mt.getKeyType)
    case _ => throw new UnsupportedOperationException(
      s"Initial type $logicalType must be a MapType")
  }

  private lazy val mapValueWriter: ArrayValueWriter = logicalType match {
    case mt: MapType => makeArrayWriter(mt.getValueType)
    case _ => throw new UnsupportedOperationException(
      s"Initial type $logicalType must be a MapType")
  }

  val ignoreNullFields = true

  private val gen = {
    val generator = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
    if (false) generator.useDefaultPrettyPrinter() else generator
  }

  private def makeWriter(logicalType: LogicalType): ValueWriter = logicalType.getTypeRoot match {
    case CHAR | VARCHAR => (row, i) => gen.writeString(row.getString(i).toString)
    case INTEGER => (row, i) => gen.writeNumber(row.getInt(i))
    case BIGINT => (row, i) => gen.writeNumber(row.getLong(i))
    case FLOAT => (row, i) => gen.writeNumber(row.getFloat(i))
    case DOUBLE => (row, i) => gen.writeNumber(row.getDouble(i))
    case ROW =>
      val fieldWriters = logicalType.asInstanceOf[RowType].getFields.asScala.map(_.getType).map(makeWriter)
      val names = logicalType.asInstanceOf[RowType].getFields.asScala.map(_.getName)
      (row, i) => {
        writeObject(writeFields(row.getRow(i, names.length), names, fieldWriters))
      }
    case ARRAY =>
      val elementWriter = makeArrayWriter(logicalType.asInstanceOf[ArrayType].getElementType)
      (row, i) => {
        writeArray(writeArrayData(row.getArray(i), elementWriter))
      }
    case MAP =>
      val keyFieldWriter = makeArrayWriter(logicalType.asInstanceOf[MapType].getKeyType)
      val valueFieldWriter = makeArrayWriter(logicalType.asInstanceOf[MapType].getValueType)
      (row, i) => {
        writeObject(writeMapData(row.getMap(i), keyFieldWriter, valueFieldWriter))
      }
    case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
  }

  private def makeArrayWriter(logicalType: LogicalType): ArrayValueWriter = logicalType.getTypeRoot match {
    case CHAR | VARCHAR => (array, i) => gen.writeString(array.getString(i).toString)
    case INTEGER => (array, i) => gen.writeNumber(array.getInt(i))
    case BIGINT => (array, i) => gen.writeNumber(array.getLong(i))
    case FLOAT => (array, i) => gen.writeNumber(array.getFloat(i))
    case DOUBLE => (array, i) => gen.writeNumber(array.getDouble(i))
    case ROW =>
      val fieldWriters = logicalType.asInstanceOf[RowType].getFields.asScala.map(_.getType).map(makeWriter)
      val names = logicalType.asInstanceOf[RowType].getFields.asScala.map(_.getName)
      (array, i) => {
        writeObject(writeFields(array.getRow(i, logicalType.asInstanceOf[RowType].getFields.size()), names, fieldWriters))
      }
    case ARRAY =>
      val elementWriter = makeArrayWriter(logicalType.asInstanceOf[ArrayType].getElementType)
      (array, i) => {
        writeArray(writeArrayData(array.getArray(i), elementWriter))
      }
    case MAP =>
      val keyFieldWriter = makeArrayWriter(logicalType.asInstanceOf[MapType].getKeyType)
      val valueFieldWriter = makeArrayWriter(logicalType.asInstanceOf[MapType].getValueType)
      (array, i) => {
        writeObject(writeMapData(array.getMap(i), keyFieldWriter, valueFieldWriter))
      }
    case _ => throw new UnsupportedOperationException(s"unsupported data type ${logicalType.getTypeRoot}")
  }

  private def writeObject(f: => Unit): Unit = {
    gen.writeStartObject()
    f
    gen.writeEndObject()
  }

  private def writeFields(
    row: RowData, names: Seq[String], fieldWriters: Seq[ValueWriter]): Unit = {
    var i = 0
    while (i < names.size) {
      val name = names(i)
      if (!row.isNullAt(i)) {
        gen.writeFieldName(name)
        fieldWriters(i).apply(row, i)
      } else if (!ignoreNullFields) {
        gen.writeFieldName(name)
        gen.writeNull()
      }
      i += 1
    }
  }

  private def writeArray(f: => Unit): Unit = {
    gen.writeStartArray()
    f
    gen.writeEndArray()
  }

  private def writeArrayData(
    array: ArrayData, fieldWriter: ArrayValueWriter): Unit = {
    var i = 0
    while (i < array.size()) {
      if (!array.isNullAt(i)) {
        fieldWriter.apply(array, i)
      } else {
        gen.writeNull()
      }
      i += 1
    }
  }

  private def writeMapData(
    map: MapData, keyFieldWriter: ArrayValueWriter, valueFieldWriter: ArrayValueWriter): Unit = {
    val keyArray = map.keyArray()
    val valueArray = map.valueArray()
    var i = 0
    while (i < map.size()) {
      gen.writeFieldName(keyFieldWriter(keyArray, i).toString)
      if (!valueArray.isNullAt(i)) {
        valueFieldWriter.apply(valueArray, i)
      } else {
        gen.writeNull()
      }
      i += 1
    }

  }

  /**
   * Transforms a single `InternalRow` to JSON object using Jackson.
   * This api calling will be validated through accessing `rootFieldWriters`.
   *
   * @param row The row to convert
   */
  def write(row: RowData): Unit = {
    writeObject(writeFields(
      row = row,
      names = rootFieldNames,
      fieldWriters = rootFieldWriters))
  }

  /**
   * Transforms multiple `InternalRow`s or `MapData`s to JSON array using Jackson
   *
   * @param array The array of rows or maps to convert
   */
  def write(array: ArrayData): Unit = writeArray(writeArrayData(array, arrElementWriter))

  /**
   * Transforms a single `MapData` to JSON object using Jackson
   * This api calling will will be validated through accessing `mapElementWriter`.
   *
   * @param map a map to convert
   */
  def write(map: MapData): Unit = {
    writeObject(writeMapData(
      keyFieldWriter = mapKeyWriter,
      valueFieldWriter = mapValueWriter,
      map = map))
  }

  def close(): Unit = gen.close()

  def flush(): Unit = gen.flush()

}
