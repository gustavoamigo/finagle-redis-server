package com.twitter.finagle.redis.server.runner

import com.twitter.finagle.redis.server.ByteArrayKey
import com.twitter.finagle.redis.server.protocol._
import com.twitter.util.Try

object CommandRunner {

  type KV = Map[ByteArrayKey, Array[Byte]]

  def run(command: Command, kv: KV): (Resp, KV) = {
    command match {
      case c: Ping => ping(c, kv)
      case c: Get => get(c, kv)
      case c: Set => set(c, kv)
      case c: Mget => mget(c, kv)
      case c: Append => append(c, kv)
      case c: Incr => incr(c, kv)
      case Unknown(err) => (ErrorResp(err), kv)
    }
  }

  def ping(ping: Ping, kv: KV):(SimpleStringResp, KV) = {
    ping.value match {
      case None =>
        (SimpleStringResp("PONG".getBytes()), kv)
      case Some(pong) =>
        (SimpleStringResp(pong), kv)
    }
  }

  def get(get: Get, kv: KV):(BulkStringResp, KV) = {
    kv.get(get.key) match {
      case Some(value) => (BulkStringResp(value), kv)
      case _ => (Resp.Nil, kv)
    }
  }

  def mget(mget: Mget, kv: KV):(ArrayResp, KV) = {
    val values = mget.keys.map(key => kv.get(key)).collect{ case Some(str) => str}
      .map(BulkStringResp.apply)
    (ArrayResp(values), kv)
  }

  // TODO: Implement TTL
  def set(set: Set, kv: KV):(SimpleStringResp, KV) = {
    (Resp.Ok, kv + ((set.key, set.value)))
  }

  def append(append: Append, kv: KV):(IntegerResp, KV) = {
    val previous = kv.getOrElse(append.key, Array.emptyByteArray)
    val value = previous ++ append.value
    val kvUpdated = kv + ((append.key, value))
    (IntegerResp(value.length), kvUpdated)
  }

  def incr(i: Incr, kv: KV):(Resp, KV) = {
    val value = kv.getOrElse(i.key, "0".getBytes)
    val numOpt = Try(new String(value).toLong).toOption
    numOpt match {
      case None => (ErrorResp("value is not an integer or out of range"), kv)
      case Some(num) =>
        val newValue =  (num + 1).toString.getBytes
        val kvUpdate = kv + ((i.key, newValue))
        (IntegerResp(num + 1), kvUpdate)
    }
  }
}
