package com.twitter.finagle.redis.server.runner

import com.twitter.finagle.redis.server.ByteArrayKey
import com.twitter.finagle.redis.server.protocol._
import com.twitter.util.Try


case class Store(
              keyValue: Map[ByteArrayKey, Array[Byte]] = Map.empty,
              keyList: Map[ByteArrayKey, Vector[Array[Byte]]] = Map.empty
            )


object CommandRunner {

  def run(command: Command, store: Store): (Resp, Store) = {
    command match {
      case c: Ping => ping(c, store)
      case c: Get => get(c, store)
      case c: Set => set(c, store)
      case c: Mget => mget(c, store)
      case c: Append => append(c, store)
      case c: Incr => incr(c, store)
      case c: Lpush => lpush(c, store)
      case Unknown(err) => (ErrorResp(err), store)
    }
  }

  def ping(ping: Ping, store: Store):(SimpleStringResp, Store) = {
    ping.value match {
      case None =>
        (SimpleStringResp("PONG".getBytes()), store)
      case Some(pong) =>
        (SimpleStringResp(pong), store)
    }
  }

  def get(get: Get, store: Store):(BulkStringResp, Store) = {
    store.keyValue.get(get.key) match {
      case Some(value) => (BulkStringResp(value), store)
      case _ => (Resp.Nil, store)
    }
  }

  def mget(mget: Mget, store: Store):(ArrayResp, Store) = {
    val values = mget.keys.map(key => store.keyValue.get(key)).collect{ case Some(str) => str}
      .map(BulkStringResp.apply)
    (ArrayResp(values), store)
  }

  // TODO: Implement TTL
  def set(set: Set, store: Store):(SimpleStringResp, Store) = {
    (Resp.Ok, store.copy(keyValue = store.keyValue + ((set.key, set.value))))
  }

  def append(append: Append, store: Store):(IntegerResp, Store) = {
    val previous = store.keyValue.get(append.key).getOrElse(Array.emptyByteArray)
    val value = previous ++ append.value
    val kvUpdated = store.keyValue + ((append.key, value))
    (IntegerResp(value.length), store.copy(keyValue = kvUpdated))
  }

  def incr(incr: Incr, store: Store):(Resp, Store) = {
    val value = store.keyValue.get(incr.key).getOrElse("0".getBytes)
    val numOpt = Try(new String(value).toLong).toOption
    numOpt match {
      case None => (ErrorResp("value is not an integer or out of range"), store)
      case Some(num) =>
        val newValue =  (num + 1).toString.getBytes
        val kvUpdate = store.keyValue + ((incr.key, newValue))
        (IntegerResp(num + 1), store.copy(keyValue = kvUpdate))
    }
  }

  def lpush(lpush: Lpush, store: Store): (Resp, Store) = {
    val list = store.keyList.get(lpush.list).getOrElse(Vector.empty)
    val updatedList = list.+:(lpush.value)
    val updatedKeyList = store.keyList + ((lpush.list, updatedList))
    (IntegerResp(updatedList.size), store.copy(keyList = updatedKeyList))
  }
}
