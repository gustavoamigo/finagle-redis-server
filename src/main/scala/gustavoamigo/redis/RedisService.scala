package gustavoamigo.redis

import com.twitter.finagle.Service
import com.twitter.io.{Buf, BufReader}
import com.twitter.util.Future
import gustavoamigo.redis.store.KeyValueStore

class RedisService(kvStore :KeyValueStore[String, Array[Byte]]) extends Service[Buf, Buf]{

  override def apply(request: Buf): Future[Buf] = {
    val startString = getStartString(request)
    val parts = startString.split(' ')
    val cmdOpt = parts.headOption

    cmdOpt match {
      case Some("GET") if parts.length == 2 =>
        val key = parts(1).trim()
        val valueF = kvStore.get(key)
        valueF.map { value =>
          val valueB = value.map(Buf.ByteArray.Owned.apply)
          valueB.getOrElse(Buf.Utf8("NOT OK\n"))
        }
      case Some("SET") if parts.length == 3 =>
        val key = parts(1).trim()
        val slice = "SET ".length + parts(1).length + 1
        val value = buf2String(request.slice(slice, request.length))
        kvStore.set(key, value).map(_ => Buf.Utf8("OK\n"))
      case _ => Future.value(Buf.Utf8("NOT OK\n"))
    }
  }

  def buf2String(buf: Buf): Array[Byte] = {
    val byteArray = new Array[Byte](buf.length)
    buf.write(byteArray, 0)
    byteArray
  }

  def getStartString(request:Buf) = {
    val size = 100
    val byteArray = new Array[Byte](size)
    val end = Math.min(request.length, size)
    val commandBuf = request.slice(0, end)
    commandBuf.write(byteArray, 0)
    String.valueOf(byteArray.map(_.toChar))
  }
}
