package scala.util

import java.util.Calendar

/**
 * 时间戳工具类, 主要操作时间戳, 大数据量时使用int/long比使用时间对象高效
 */
object TsUtils {
  val SECOND_UNIT = 1000L
  val MINUTE_UNIT = 1000L * 60
  val MINUTE_UNIT5 = 1000L * 60 * 5
  val MINUTE_UNIT10 = 1000L * 60 * 10
  val HOUR_UNIT = 1000L * 60 * 60
  val DAY_UNIT = 1000L * 60 * 60 * 24

  /**
   * The number of days in a 400 year cycle.
   */
  private val DAYS_PER_CYCLE = 146097
  /**
   * The number of days from year zero to year 1970.
   * There are five 400 year cycles from year zero to 2000.
   * There are 7 leap years from 1970 to 2000.
   */
  private val DAYS_0000_TO_1970 = (DAYS_PER_CYCLE * 5) - (30 * 365 + 7)

  private val START_TS_1970: Long = {
    val cal = Calendar.getInstance
    cal.set(Calendar.YEAR, 1970)
    cal.set(Calendar.MONTH, 0)
    cal.set(Calendar.DAY_OF_MONTH, 1)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }

  // (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
  private val WEEK_DAY_1970: Int = {
    val cal = Calendar.getInstance
    cal.set(Calendar.YEAR, 1970)
    cal.set(Calendar.MONTH, 0)
    cal.set(Calendar.DAY_OF_MONTH, 1)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)

    // (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
    cal.get(Calendar.DAY_OF_WEEK) + 5 % 7
  }

  /* 时间戳规约到整点整分 */

  def tsIntervalMin(ts: Long, intervalMin: Int): Long = {
    val interval = MINUTE_UNIT * intervalMin
    ts / interval * interval
  }

  def tsIntervalMin(ts: Long): Long = {
    ts / MINUTE_UNIT * MINUTE_UNIT
  }

  def tsInterval5Min(ts: Long): Long = {
    ts / MINUTE_UNIT5 * MINUTE_UNIT5
  }

  def tsInterval10Min(ts: Long): Long = {
    ts / MINUTE_UNIT10 * MINUTE_UNIT10
  }

  def tsIntervalHour(ts: Long): Long = {
    ts / HOUR_UNIT * HOUR_UNIT
  }

  def daysFrom1970(ts: Long): Int = {
    // long.toInt 编译成class字节码时, 会对原生long强转为int, 不会额外生成对象调用对象的方法
    if(ts >=  START_TS_1970){
      ((ts - START_TS_1970) / DAY_UNIT).toInt
    }else{
      - ((START_TS_1970 - ts - 1) / DAY_UNIT).toInt - 1
    }
  }

  def hourOfDay(ts: Long): Int = {
    if(ts >=  START_TS_1970){
      ((ts - START_TS_1970) % DAY_UNIT / HOUR_UNIT).toInt
    }else{
      (23 - (START_TS_1970 - ts - 1) % DAY_UNIT / HOUR_UNIT).toInt
    }
  }

  def minuteOfHour(ts: Long): Int = {
    if(ts >=  START_TS_1970){
      (ts / MINUTE_UNIT % 60).toInt
    }else{
      (59 - (START_TS_1970 - ts - 1) % HOUR_UNIT / MINUTE_UNIT ).toInt
    }
  }

  def secondOfMinute(ts: Long): Int = {
    if(ts >=  START_TS_1970){
      (ts / SECOND_UNIT % 60).toInt
    }else{
      (59 - (START_TS_1970 - ts - 1) % MINUTE_UNIT / SECOND_UNIT ).toInt
    }
  }

  // (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
  def weekDay(ts: Long): Int = {
    val day = ((ts - START_TS_1970) / DAY_UNIT).toInt
    (WEEK_DAY_1970 + day) % 7
  }

  def date : Date = dateFromEpochDay(daysFrom1970(System.currentTimeMillis()))

  def date(ts: Long): Date = dateFromEpochDay(daysFrom1970(ts))

  // 元组只有1-2标记specialized, 定义Date
  def dateFromEpochDay(epochDay: Int): Date = {
    var zeroDay = epochDay + DAYS_0000_TO_1970
    // find the march-based year
    zeroDay -= 60 // adjust to 0000-03-01 so leap day is at end of four year cycle

    var adjust = 0
    if (zeroDay < 0) { // adjust negative years to positive for calculation
      val adjustCycles = (zeroDay + 1) / DAYS_PER_CYCLE - 1
      adjust = adjustCycles * 400
      zeroDay += -adjustCycles * DAYS_PER_CYCLE
    }
    var yearEst = (400 * zeroDay + 591) / DAYS_PER_CYCLE
    var doyEst = zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400)
    if (doyEst < 0) { // fix estimate
      yearEst -= 1
      doyEst = zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400)
    }
    yearEst += adjust // reset any negative year

    val marchDoy0 = doyEst

    // convert march-based values back to january-based
    val marchMonth0 = (marchDoy0 * 5 + 2) / 153
    val month = (marchMonth0 + 2) % 12 + 1
    val dom = marchDoy0 - (marchMonth0 * 306 + 5) / 10 + 1
    yearEst += marchMonth0 / 10

    Date(yearEst, month, dom)
  }

  def timestamp(ts:Long): Timestamp ={
    val epochDay = daysFrom1970(ts)
    var zeroDay = epochDay + DAYS_0000_TO_1970
    // find the march-based year
    zeroDay -= 60 // adjust to 0000-03-01 so leap day is at end of four year cycle

    var adjust = 0
    if (zeroDay < 0) { // adjust negative years to positive for calculation
      val adjustCycles = (zeroDay + 1) / DAYS_PER_CYCLE - 1
      adjust = adjustCycles * 400
      zeroDay += -adjustCycles * DAYS_PER_CYCLE
    }
    var yearEst = (400 * zeroDay + 591) / DAYS_PER_CYCLE
    var doyEst = zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400)
    if (doyEst < 0) { // fix estimate
      yearEst -= 1
      doyEst = zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400)
    }
    yearEst += adjust // reset any negative year

    val marchDoy0 = doyEst

    // convert march-based values back to january-based
    val marchMonth0 = (marchDoy0 * 5 + 2) / 153
    val month = (marchMonth0 + 2) % 12 + 1
    val dom = marchDoy0 - (marchMonth0 * 306 + 5) / 10 + 1
    yearEst += marchMonth0 / 10

    val hour = hourOfDay(ts)
    val minute = minuteOfHour(ts)
    val second = secondOfMinute(ts)

    Timestamp(yearEst, month, dom, hour, minute, second)
  }

  // 元组只有1-2标记specialized, 定义Date
  case class Date(year: Int, month: Int, dayOfMonth: Int) {
    override def toString: String = {
      val absYear = math.abs(year)
      val buf = new java.lang.StringBuilder(10)

      if (absYear < 1000) {
        if (year < 0) {
          buf.append(year - 10000).deleteCharAt(1);
        } else {
          buf.append(year + 10000).deleteCharAt(0);
        }
      } else {
        if (year > 9999) {
          buf.append('+')
        }
        buf.append(year)
      }

      buf.append(if (month < 10) "-0" else "-")
        .append(month)
        .append(if (dayOfMonth < 10) "-0" else "-")
        .append(dayOfMonth)
        .toString()
    }
  }

  case class Timestamp(year: Int, month: Int, dayOfMonth: Int, hour: Int, minute: Int, second: Int) {
    override def toString: String = {
      val absYear = math.abs(year)
      val buf = new java.lang.StringBuilder(10)

      if (absYear < 1000) {
        if (year < 0) {
          buf.append(year - 10000).deleteCharAt(1);
        } else {
          buf.append(year + 10000).deleteCharAt(0);
        }
      } else {
        if (year > 9999) {
          buf.append('+')
        }
        buf.append(year)
      }

      buf.append(if (month < 10) "-0" else "-")
        .append(month)
        .append(if (dayOfMonth < 10) "-0" else "-")
        .append(dayOfMonth)
        .append(if (hour < 10) " 0" else " ")
        .append(hour)
        .append(if (minute < 10) ":0" else ":")
        .append(minute)
        .append(if (second < 10) ":0" else ":")
        .append(second)
        .toString()
    }

    def toDateString: String = {
      val absYear = math.abs(year)
      val buf = new java.lang.StringBuilder(10)

      if (absYear < 1000) {
        if (year < 0) {
          buf.append(year - 10000).deleteCharAt(1);
        } else {
          buf.append(year + 10000).deleteCharAt(0);
        }
      } else {
        if (year > 9999) {
          buf.append('+')
        }
        buf.append(year)
      }

      buf.append(if (month < 10) "-0" else "-")
        .append(month)
        .append(if (dayOfMonth < 10) "-0" else "-")
        .append(dayOfMonth)
        .toString()
    }
  }



  def main(args: Array[String]): Unit = {
    val cal = Calendar.getInstance
    cal.set(Calendar.YEAR, 1970)
    cal.set(Calendar.MONTH, 0)
    cal.set(Calendar.DAY_OF_MONTH, 1)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)

    println(cal)
    // (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
    println(cal.get(Calendar.DAY_OF_WEEK))
  }
}
