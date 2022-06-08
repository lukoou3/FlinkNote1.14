package scala.util

import java.{util => ju}

import scala.log.Logging


/**
 * 主要用于实现全局对象，不适用于数据库连接池等
 * 主要用于flink算子中，方便复用全局对象
 */
object SingleValueMap extends Logging{
  sealed trait Data[T]{
    def key: String
    def data: T
  }

  class ResourceData[T](val key: String, val data: T, releaseFunc: T => Unit) extends Data[T]{
    private[SingleValueMap] var useCnt = 0

    private[SingleValueMap] def inUse: Boolean = useCnt > 0

    private[SingleValueMap] def release(): Unit = releaseFunc(data)

    override def toString: String = s"ResourceData(key=$key, data=$data, useCnt=$useCnt)"
  }

  class NonResourceData[T](val key: String, val data: T) extends Data[T]{
    override def toString: String = s"NonResourceData(key=$key, data=$data)"
  }

  private lazy val cache: ju.Map[String, Data[_]] = new ju.LinkedHashMap[String, Data[_]]

  def acquireResourceData[T](key: String, createData: => T)(releaseFunc: T => Unit): ResourceData[T] = synchronized {
    val existingData = cache.get(key)
    val data = if(existingData == null){
      val newData =  new ResourceData[T](key, createData, releaseFunc)
      cache.put(key, newData)
      newData
    }else{
      existingData.asInstanceOf[ResourceData[T]]
    }
    data.useCnt += 1

    logInfo(s"acquireResourceData: $data")

    data
  }

  def acquireNonResourceData[T](key: String, createData: => T): NonResourceData[T] = synchronized {
    val existingData = cache.get(key)
    if(existingData == null){
      val newData =  new NonResourceData[T](key, createData)
      cache.put(key, newData)
      newData
    }else{
      existingData.asInstanceOf[NonResourceData[T]]
    }
  }

  def release[T](data: ResourceData[T]): Unit = synchronized {
    val cachedData = cache.get(data.key)
    assert(data eq cachedData)

    logInfo(s"releaseResourceData: $data")

    data.useCnt -= 1
    if(!data.inUse){
      data.release()
      cache.remove(data.key)

      logInfo(s"releaseAndRemoveResourceData: $data")
    }
  }

}
