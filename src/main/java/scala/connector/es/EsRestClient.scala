package scala.connector.es

import scalaj.http.{Http, HttpRequest, HttpResponse}

import scala.log.Logging

class EsRestClient(
  val nodes: String,
  val user: Option[String],
  val password: Option[String]
) extends Logging {
  val hosts = nodes.split(",").map(_.trim).filter(_.nonEmpty).map(host => s"http://$host/")
  var host = hosts(0)
  var nextHost = 1
  val headers = Map("content-type" -> "application/json") ++ {
    if(user.isEmpty)
      Map.empty
    else
      Map("Authorization"-> EsSecurity.basicAuthHeaderValue(user.get, password.get))
  }

  def get[T](path:String, connTimeoutMs: Int = 1000 * 3, readTimeoutMs: Int = 1000 * 3)(func:(Int, String) => T): T = {
    val response: HttpResponse[String] = retryRequest{
      val url = host + (if(path.startsWith("/")) path.substring(1) else path)
      val request: HttpRequest = Http(url).headers(headers).timeout(connTimeoutMs, readTimeoutMs)
      request.asString
    }
    func(response.code, response.body)
  }

  def post[T](path:String, body: String = null, connTimeoutMs: Int = 1000 * 3, readTimeoutMs: Int = 1000 * 5)(func:(Int, String) => T): T = {
    val response: HttpResponse[String] = retryRequest{
      val url = host + (if(path.startsWith("/")) path.substring(1) else path)
      val request: HttpRequest = if (body == null) {
        Http(url).headers(headers).method("POST").timeout(connTimeoutMs, readTimeoutMs)
      } else {
        Http(url).headers(headers).postData(body).timeout(connTimeoutMs, readTimeoutMs)
      }
      request.asString
    }
    func(response.code, response.body)
  }

  private def selectNextHost(): Boolean = {
    if (nextHost >= hosts.size) {
      false
    } else {
      host = hosts(nextHost)
      nextHost += 1
      if(nextHost >= hosts.size){
        nextHost = 0
      }
      true
    }
  }

  private def retryRequest[T](func: => T): T = {
    var i = 0
    while(i < 2) {
      try {
        return func
      } catch {
        case e:Exception =>
          logError("请求出错", e)
          selectNextHost()
          i += 1
      }
    }
    throw new Exception("多次失败")
  }

}
