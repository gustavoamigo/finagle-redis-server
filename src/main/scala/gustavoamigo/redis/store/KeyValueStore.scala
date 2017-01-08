package gustavoamigo.redis.store

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.util.Future

class KeyValueStore[K,V] {
  private val getterReq = new Broker[K]
  private val getterRes = new Broker[Option[V]]
  private val setterReq = new Broker[(K, V)]



  private def loop(kv: Map[K, V]): Unit = {
    Offer.select(
      getterReq.recv.map { k => getterRes.send(kv.get(k)).sync().map(_ => loop(kv)) },
      setterReq.recv.map { case (k,v) => loop(kv + ((k,v))) }
    )
  }

  loop(Map.empty)

  val kv = new ConcurrentHashMap[K,V]

  def get(k: K): Future[Option[V]] = {
    for {
      _ <- getterReq ! k
      v <- getterRes ?
    } yield {
      v
    }
  }

  def set(k: K, v: V): Future[Unit] = {
    setterReq ! (k,v)
  }
}
