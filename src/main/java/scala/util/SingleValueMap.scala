package scala.util

import java.{util => ju}

import scala.log.Logging
import scala.collection.JavaConverters._

/**
 * 主要用于实现全局对象，不适用于数据库连接池等
 */
object SingleValueMap extends Logging {
  sealed trait Data[T]{
    def key: Any
    def data: T
  }

  final class ResourceData[T] (val key: Any, val data: T, destroyFunc: T => Unit) extends Data[T]{
    private[SingleValueMap] var useCnt = 0

    private[SingleValueMap] def inUse: Boolean = useCnt > 0

    private[SingleValueMap] def destroy(): Unit = destroyFunc(data)

    def release(): Unit = releaseResourceData(this)

    override def toString: String = s"ResourceData(key=$key, data=$data, useCnt=$useCnt)"
  }

  final class NonResourceData[T](val key: Any, val data: T) extends Data[T]{
    override def toString: String = s"NonResourceData(key=$key, data=$data)"
  }

  private lazy val cache: ju.Map[Any, Data[_]] = new ju.LinkedHashMap[Any, Data[_]]

  def acquireResourceData[T](key: Any, createData: => T)(releaseFunc: T => Unit): ResourceData[T] = synchronized {
    val existingData = cache.get(key)
    val data = if(existingData == null){
      val newData =  new ResourceData[T](key, createData, releaseFunc)
      cache.put(key, newData)
      newData
    }else{
      existingData.asInstanceOf[ResourceData[T]]
    }
    data.useCnt += 1

    logWarning(s"acquireResourceData: $data")

    data
  }

  def acquireNonResourceData[T](key: Any, createData: => T): NonResourceData[T] = synchronized {
    val existingData = cache.get(key)
    if(existingData == null){
      val newData =  new NonResourceData[T](key, createData)
      cache.put(key, newData)
      newData
    }else{
      existingData.asInstanceOf[NonResourceData[T]]
    }
  }

  private def releaseResourceData[T](data: ResourceData[T]): Unit = synchronized {
    val cachedData = cache.get(data.key)
    if(cachedData == null){
      return
    }

    assert(data eq cachedData)

    logWarning(s"releaseResourceData: $data")

    data.useCnt -= 1
    if(!data.inUse){
      data.destroy()
      cache.remove(data.key)

      logWarning(s"releaseAndRemoveResourceData: $data")
    }
  }

  def close(): Unit = synchronized {
    val datas = cache.values().asScala.collect{case data: ResourceData[_] => data}.toArray
    for (data <- datas) {
      data.destroy()
      cache.remove(data.key)
    }
  }
}
