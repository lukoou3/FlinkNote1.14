package scala.sql.udf

import org.apache.flink.table.functions.ScalarFunction

class SubstringIndex extends ScalarFunction {
  def eval(str: String, delim: String, count: Int): String = {
    if (str == null || delim == null) {
      return null
    }

    if (delim == "" || count == 0) {
      return ""
    }

    if (count > 0) {
      subStringIndex(str, delim, count)
    } else {
      // 先这样吧, 逻辑简单, 负索引用的也不多
      subStringIndex(str.reverse, delim.reverse, -count).reverse
    }
  }

  def subStringIndex(str: String, delim: String, cnt: Int): String = {
    var idx = -1
    var count = cnt
    while (count > 0) {
      idx = str.indexOf(delim, idx)
      if (idx >= 0) {
        count -= 1
      } else {
        // 找不到delim
        return str
      }
    }

    if (idx == 0) "" else str.substring(0, idx)
  }
}
