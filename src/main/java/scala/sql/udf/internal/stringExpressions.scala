package scala.sql.udf.internal

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.{ArrayData, GenericArrayData, MapData, RowData, StringData}
import org.apache.flink.table.data.binary.{BinaryArrayData, BinaryStringData, BinaryStringDataUtil}
import org.apache.flink.table.types.{CollectionDataType, DataType}
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.inference.{ArgumentCount, CallContext}
import org.apache.flink.table.types.logical.{ArrayType, MultisetType, VarCharType}

import scala.collection.JavaConverters._
import InternalScalarFunction._
import scala.annotation.varargs
import scala.collection.Iterator
import scala.log.Logging

class SubstringIndex extends InternalScalarFunction {

  def eval(str: StringData, delim: StringData, count: Int): StringData = {
    if (str == null || delim == null) {
      return null
    }

    if(delim.asInstanceOf[BinaryStringData] == BinaryStringData.EMPTY_UTF8 || count == 0){
      return BinaryStringData.EMPTY_UTF8
    }

    if (count > 0) {
      subStringIndex(str.asInstanceOf[BinaryStringData], delim.asInstanceOf[BinaryStringData], count)
    }else {
      // 先这样吧, 逻辑简单, 负索引用的也不多
      BinaryStringDataUtil.reverse(subStringIndex(
        BinaryStringDataUtil.reverse(str.asInstanceOf[BinaryStringData]),
        BinaryStringDataUtil.reverse(delim.asInstanceOf[BinaryStringData]),
        -count))
    }
  }

  def subStringIndex(str: BinaryStringData, delim: BinaryStringData, cnt: Int): BinaryStringData = {
    var idx = -1
    var count = cnt
    while (count > 0) {
      idx = str.indexOf(delim, idx + 1)
      if (idx >= 0) {
        count -= 1
      } else {
        // 找不到delim
        return str
      }
    }

    if (idx == 0) BinaryStringData.EMPTY_UTF8 else str.substring(0, idx)
  }

  override def argumentCount: ArgumentCount = fixArgumentCount(3)

  override def stringArgs: Seq[String] = Seq("str", "delim", "count")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] =
    Seq(stringDateType, stringDateType, intDateType)

  override def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = stringDateType
}

// org.apache.spark.sql.catalyst.expressions.ConcatWs
class ConcatWs extends InternalScalarFunction{
  @transient lazy val valueGetter = ArrayData.createElementGetter(DataTypes.STRING().getLogicalType)

  @varargs
  def eval(eles: AnyRef*): StringData = {
    val flatInputs = eles.tail.toIterator.flatMap { ele =>
      ele match {
        case null => Iterator(null)
        case s: StringData => Iterator(s)
        case arr: ArrayData => arr match {
          case array: GenericArrayData => array.toObjectArray.asInstanceOf[Array[StringData]]
          case array: BinaryArrayData => array.toObjectArray[StringData](new VarCharType(Integer.MAX_VALUE))
        }
        case map: MapData =>
          val valueArray = map.keyArray()
          val cntArray = map.valueArray()
          (0 until map.size()).flatMap{ i =>
            val value = valueGetter.getElementOrNull(valueArray, i)
            val cnt = cntArray.getInt(i)
            //Array.fill(cnt)(value)
            Iterator.fill(cnt)(value)
          }
      }
    }.asInstanceOf[Iterator[BinaryStringData]]

    val head = eles(0)
    // concatWs: null值会直接过滤
    BinaryStringDataUtil.concatWs(if(head == null) null else head.asInstanceOf[BinaryStringData], flatInputs.toIterable.asJava)
  }

  override def argumentCount: ArgumentCount = anyArgumentCount

  override def stringArgs: Seq[String] = Seq("sep[, str | array|multiset(str)]")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = {
    args.map{ arg =>
      arg.getLogicalType.getTypeRoot match {
        case ARRAY => new CollectionDataType(new ArrayType(new VarCharType(Integer.MAX_VALUE)), DataTypes.STRING())
        case MULTISET => new CollectionDataType(new MultisetType(new VarCharType(Integer.MAX_VALUE)), DataTypes.STRING())
        case _ => DataTypes.STRING()
      }
    }
  }

  override def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = DataTypes.STRING()
}

class UrlDecode extends InternalScalarFunction with Logging{
  var enc = "UTF-8"

  def eval(url: StringData): StringData = {
    if(url == null){
      null
    }else{
      try {
        val decodedUrl = java.net.URLDecoder.decode(url.toString, this.enc)
        StringData.fromString(decodedUrl)
      } catch {
        case e:Exception =>
          println(s"UrlDecode失败 for:$url.toString")
          logError(s"UrlDecode失败 for:$url.toString", e)
          url
      }
    }
  }

  def eval(url: StringData, enc: StringData): StringData = {
    eval(url)
  }

  override def argumentCount: ArgumentCount = betweenArgumentCount(1, 2)

  override def stringArgs: Seq[String] = Seq("url", "enc")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = args.map(_ => stringDateType)

  override def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    if(args.length == 2){
      assert(callContext.isArgumentLiteral(1), "enc必须是字面量")
      enc = callContext.getArgumentValue(1,  classOf[String]).get()
    }

    stringDateType
  }
}

class UrlDecode2 extends InternalScalarFunction with Logging {
  var decodeCount = 2
  var enc = "UTF-8"

  def eval(url: StringData, decodeCnt :Int): StringData = {
    if(url == null){
      null
    }else{
      var decodedUrl = url.toString
      try {
        var i = 0
        while (i < decodeCnt) {
          decodedUrl = java.net.URLDecoder.decode(decodedUrl, this.enc)
          i += 1
        }
      } catch {
        case e:Exception =>
          println(s"UrlDecode失败 for:$decodedUrl , first is:$url.toString")
          logError(s"UrlDecode失败 for:$decodedUrl , first is:$url.toString", e)
      }
      StringData.fromString(decodedUrl)
    }
  }

  def eval(url: StringData, decodeCnt :Int, enc: StringData): StringData = {
    eval(url, decodeCnt)
  }

  override def argumentCount: ArgumentCount = betweenArgumentCount(2, 3)

  override def stringArgs: Seq[String] = Seq("url", "decode_count", "enc")

  override def inferInputTypes(args: Seq[DataType], callContext: CallContext): Seq[DataType] = args.zipWithIndex.map{case(_, i) =>
    if(i==1) intDateType else stringDateType
  }

  override def inferOutputType(args: Seq[DataType], callContext: CallContext, typeFactory: DataTypeFactory): DataType = {
    assert(callContext.isArgumentLiteral(1), "decode_count必须是字面量")
    decodeCount = callContext.getArgumentValue(1,  classOf[java.lang.Integer]).get()
    if(args.length == 3){
      assert(callContext.isArgumentLiteral(2), "enc必须是字面量")
      enc = callContext.getArgumentValue(2,  classOf[String]).get()
    }

    stringDateType
  }
}