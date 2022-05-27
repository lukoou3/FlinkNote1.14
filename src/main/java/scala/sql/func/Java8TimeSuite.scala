package scala.sql.func

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.Date

import org.scalatest.funsuite.AnyFunSuite

class Java8TimeSuite extends AnyFunSuite {

  /**
   * Instant类似于unix时间戳, 里面就保存了时间戳信息
   */
  test("Instant"){
    val instant = Instant.now()
    val seconds = instant.getEpochSecond
    val milli = instant.toEpochMilli
    val ts = System.currentTimeMillis()

    /**
     * (instant,2022-05-27T05:47:21.813Z)
     * (date,2022-05-27 13:47:21.813)
     * (seconds,1653630441)
     * (milli,1653630441813)
     * (ts,1653630441813)
     */
    println("instant", instant)
    println("date", new Timestamp(ts))
    println("seconds", seconds)
    println("milli", milli)
    println("ts", ts)
  }

  /**
   * 看不出来这个类有啥好，还不如date方便
   */
  test("LocalDateTime"){
    val dateTime = LocalDateTime.now() // systemDefaultZone
    println(dateTime)
    val second = LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"))
    println(second)
    println(System.currentTimeMillis() / 1000)
  }

  /**
   * 这个日期fmt是线程安全的
   */
  test("DateTimeFormatter"){
    val dateTime = LocalDateTime.now()
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = fmt.format(dateTime)
    val temporalAccessor = fmt.parse(dateStr)
    // 还得这么调用才会返回LocalDateTime
    val dateTime2 = LocalDateTime.parse(dateStr, fmt)

    println("dateStr", dateStr)
    println("temporalAccessor", temporalAccessor)
    println("dateTime2", dateTime2)
  }
}
