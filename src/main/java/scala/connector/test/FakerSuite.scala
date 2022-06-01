package scala.connector.test

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.github.javafaker.Faker
import org.scalatest.funsuite.AnyFunSuite

class FakerSuite extends AnyFunSuite{

  test("test en"){
    val faker = new Faker(Locale.ENGLISH)
    val fieldExpression = "#{date.past '15','SECONDS'}"
    println(faker.expression(fieldExpression))
    println(faker.expression("#{Name.first_name}"))
    println(faker.expression("#{Name.last_name}"))
    println(faker.expression("#{Name.name}"))

    val fmt = "EEE MMM dd HH:mm:ss zzz yyyy"
    val dateTimeFormatter = DateTimeFormatter.ofPattern(fmt, Locale.US)
    val localDateTime = LocalDateTime.parse(faker.expression(fieldExpression), dateTimeFormatter)
    println(localDateTime)
  }

  test("test zh"){
    val faker = new Faker(Locale.CHINA)
    val fieldExpression = "#{date.past '15','SECONDS'}"
    println(faker.expression(fieldExpression))
    println(faker.expression("#{Name.first_name}"))
    println(faker.expression("#{Name.last_name}"))
    println(faker.expression("#{Name.name}"))
  }

  test("test date fmt2"){
    val faker = new Faker(Locale.ENGLISH)
    val fieldExpression = "#{date.past '15','SECONDS'}"
    val fmt = "EEE MMM dd HH:mm:ss zzz yyyy"
    val dateTimeFormatter = DateTimeFormatter.ofPattern(fmt, Locale.ENGLISH)
    val localDateTime = LocalDateTime.parse(faker.expression(fieldExpression), dateTimeFormatter)
    println(localDateTime)



    val fmt2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    println(fmt2.format(localDateTime))

    val instant = Instant.from(dateTimeFormatter.parse(faker.expression(fieldExpression)))
    println(new Timestamp(instant.toEpochMilli))
    println(new Timestamp(localDateTime.toEpochSecond(ZoneOffset.of("+8")) * 1000))


  }

  /**
   * Date formats¶
   * Since 1.2.0 Datafaker supports specifying of date formats for dates and timestamps.
   * Faker faker = new Faker();
   * System.out.println(faker.date().future(1, TimeUnit.HOURS, "YYYY MM.dd mm:hh:ss");
   * System.out.println(faker.date().past(1, TimeUnit.HOURS, "YYYY-MM-dd mm:hh:ss");
   * System.out.println(faker.date().birthday(1, 99, "YYYY/MM/dd");
   * And also this feature could be used in expressions like
   * faker.expression("#{date.past '15','SECONDS','dd/MM/yyyy hh:mm:ss'}")
   */
  test("test date fmt"){
    val faker = new Faker(Locale.ENGLISH)
    val fieldExpression = "#{date.past '15','SECONDS','dd/MM/yyyy hh:mm:ss'}"
    println(faker.expression("#{date.past '15','SECONDS','YYYY-MM-dd mm:hh:ss'}"))
  }



}
