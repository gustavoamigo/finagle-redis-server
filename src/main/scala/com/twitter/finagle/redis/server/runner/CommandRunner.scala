package com.twitter.finagle.redis.server.runner

import com.twitter.finagle.redis.server.ByteArrayWrapper
import com.twitter.finagle.redis.server.protocol._

sealed trait Value
case class StringValue(str: ByteArrayWrapper) extends Value
case class IntegerValue(int: Int) extends Value
case class ListValue(list: List[Value]) extends Value

object CommandRunner {

  type KV = Map[ByteArrayWrapper, Value]

  def run(command: Command, kv: KV): (Resp, KV) = {
    command match {
      case c: Ping => ping(c, kv)
      case c: Get => get(c, kv)
      case c: Set => set(c, kv)
      case c: Mget => mget(c, kv)
      case c: Append => append(c, kv)
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
      case Some(StringValue(value)) => (BulkStringResp(value), kv)
      case _ => (Resp.Nil, kv)
    }
  }

  def mget(mget: Mget, kv: KV):(ArrayResp, KV) = {
    val values = mget.keys.map(key => kv.get(key))
      .collect { case Some(StringValue(e)) => e }
      .map(BulkStringResp.apply)
    (ArrayResp(values), kv)
  }

  // TODO: Implement TTL
  def set(set: Set, kv: KV):(SimpleStringResp, KV) = {
    (Resp.Ok, kv + ((set.key, StringValue(set.value))))
  }

  def append(append: Append, kv: KV):(IntegerResp, KV) = {
    val previous = kv.get(append.key).collect { case StringValue(str) => str}.getOrElse(ByteArrayWrapper.empty)
    val value = previous ++ append.value
    val kvUpdated = kv + ((append.key, StringValue(value)))
    (IntegerResp(value.length), kvUpdated)
  }
}
