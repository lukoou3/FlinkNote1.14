package scala.connector.test

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.util.Locale

import net.datafaker.Faker
import org.scalatest.funsuite.AnyFunSuite

/**
 * java的和python的faker库功能还差很多，不过比没有强点
 * java新的faker库比之前的好点，时间是标准格式。https://github.com/datafaker-net/datafaker/
 */
class FakerSuite extends AnyFunSuite{

  test("test en"){
    val faker = new Faker(Locale.ENGLISH)
    val fieldExpression = "#{date.past '15','SECONDS'}"
    println(faker.expression(fieldExpression))
    println(faker.expression("#{Name.first_name}"))
    println(faker.expression("#{Name.last_name}"))
    println(faker.expression("#{Name.name}"))

    //val fmt = "EEE MMM dd HH:mm:ss zzz yyyy"
    val fmt = "yyyy-MM-dd HH:mm:ss.SSS"
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
    //val fmt = "EEE MMM dd HH:mm:ss zzz yyyy"
    val fmt = "yyyy-MM-dd HH:mm:ss.SSS"
    val dateTimeFormatter = DateTimeFormatter.ofPattern(fmt, Locale.ENGLISH)
    val localDateTime = LocalDateTime.parse(faker.expression(fieldExpression), dateTimeFormatter)
    println(localDateTime)

    val fmt2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    println(fmt2.format(localDateTime))

    //val instant = Instant.from(dateTimeFormatter.parse(faker.expression(fieldExpression)))
    //println(new Timestamp(instant.toEpochMilli))
    println(new Timestamp(localDateTime.toEpochSecond(ZoneOffset.of("+8")) * 1000))

  }

  test("test date fmt3"){
    val faker = new Faker(Locale.ENGLISH)
    val FORMATTER = new DateTimeFormatterBuilder()
        // Pattern was taken from java.sql.Timestamp#toString
        //.appendPattern("uuuu-MM-dd HH:mm:ss")
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
        .toFormatter(Locale.US);

    val dateStr = faker.expression("#{date.past '15','SECONDS'}")
    println(LocalDateTime.parse(dateStr, FORMATTER))
    println(LocalDateTime.parse(dateStr, FORMATTER.withZone(ZoneId.systemDefault())))

    val instant = Instant.from(FORMATTER.withZone(ZoneId.systemDefault()).parse(dateStr))
    println(new Timestamp(instant.toEpochMilli))
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
    println(faker.expression("#{date.past '15','SECONDS','dd/MM/yyyy mm:hh:ss'}"))
  }

  test("faker test"){
    val faker = new Faker(Locale.CHINA)

    println("name().fullName", faker.name().fullName())
    println("name().name", faker.name().name())
    println("name().firstName", faker.name().firstName())
    println("name().lastName", faker.name().lastName())
    println("#{Name.first_name}", faker.expression("#{Name.first_name}"))
    println("#{Name.last_name}", faker.expression("#{Name.last_name}"))
    println("#{Name.name}", faker.expression("#{Name.name}"))

    println("country().name", faker.country().name())
    println("address().country", faker.address().country())
    println("address().city", faker.address().city())
    println("address().cityName", faker.address().cityName())
    println("address().streetAddress", faker.address().streetAddress())
    println("address().streetName", faker.address().streetName())
    println("address().streetName", faker.address().streetAddressNumber())

    println("#{Address.city}", faker.expression("#{Address.city}"))
    println("#{Address.city_name}", faker.expression("#{Address.city_name}"))
  }

  test("faker test2"){
    val faker = new Faker(Locale.CHINA)

    println("phoneNumber().phoneNumber", faker.phoneNumber().phoneNumber())//座机号码
    println("phoneNumber().cellPhone", faker.phoneNumber().cellPhone())//手机号

    println("#{PhoneNumber.phoneNumber}", faker.expression("#{PhoneNumber.phoneNumber}"))
    println("#{PhoneNumber.cellPhone}", faker.expression("#{PhoneNumber.cellPhone}"))

    println(faker.job().title())
    println(faker.color().name())
    println(faker.color().name())
  }


}
