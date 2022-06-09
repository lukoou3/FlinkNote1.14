package scala.util

import java.util.{Optional => JOption}

object Converters {

  // scala/java的Option相互转化
  implicit final class RichJOption[T](val optional: JOption[T]) {

    def asScala: Option[T] = optional match {
      case null => null
      case _ => if (optional.isPresent) Option(optional.get()) else None
    }

  }

  // scala转java时，可以直接调用：Optional.ofNullable(option.orNull)
  implicit final class RichOption[T](val option: Option[T]) {

    def asJava: JOption[T] = option match {
      case null => null
      case _ => if (option.isDefined) JOption.ofNullable(option.get) else JOption.empty()
    }

  }

}
