package scala.sql.udf

import java.util.regex.{MatchResult, Matcher, Pattern}

import org.apache.flink.table.functions.ScalarFunction

class StringSplit extends ScalarFunction {
  def eval(str: String, regex: String): Array[String] = {
    if (str == null || regex == null) {
      null
    } else {
      str.split(regex)
    }
  }
}

class RegExpExtract extends ScalarFunction {
  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _

  def eval(str: String, regex: String, idx: Int): String = {
    if (str == null || regex == null) {
      return null
    }

    val m = getLastMatcher(str, regex)
    // 只要参数不为null, 匹配不到返回的是空字符串
    if (m.find) {
      val mr: MatchResult = m.toMatchResult
      RegExpExtractBase.checkGroupIndex(mr.groupCount, idx)
      val group = mr.group(idx)
      if (group == null) { // Pattern matched, but it's an optional group
        ""
      } else {
        group
      }
    } else {
      ""
    }
  }

  protected def getLastMatcher(str: String, regex: String): Matcher = {
    // 正常情况下我们输入的正则字符串都是字面量, 这个每个task的每个类只会执行一次初始化pattern
    if (regex != lastRegex) {
      // regex value changed
      lastRegex = regex
      pattern = Pattern.compile(lastRegex)
    }
    pattern.matcher(str)
  }
}

object RegExpExtractBase {
  def checkGroupIndex(groupCount: Int, groupIndex: Int): Unit = {
    if (groupIndex < 0) {
      throw new IllegalArgumentException("The specified group index cannot be less than zero")
    } else if (groupCount < groupIndex) {
      throw new IllegalArgumentException(
        s"Regex group count is $groupCount, but the specified group index is $groupIndex")
    }
  }
}