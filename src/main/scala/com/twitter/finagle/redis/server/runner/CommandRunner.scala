package com.twitter.finagle.redis.server.runner

import com.twitter.finagle.redis.server.ByteArrayWrapper
import com.twitter.finagle.redis.server.protocol._

sealed trait Value
case class StringValue(str: ByteArrayWrapper) extends Value
case class IntegerValue(int: Int) extends Value
case class ListValue(list: List[Value]) extends Value

object CommandRunner {

  type KV = Map[ByteArrayWrapper, Value]

  def run(command: Command, kv: KV): (Reply, KV) = {
    command match {
      case c: Get => get(c, kv)
      case c: Set => set(c, kv)
      case c: Mget => mget(c, kv)
      case c: Append => append(c, kv)
      case Unknown(err) => (Error(err), kv)
    }
  }

  def get(get: Get, kv: KV):(BulkStrings, KV) = {
    kv.get(get.key) match {
      case Some(StringValue(value)) => (BulkStrings(value), kv)
      case _ => (Reply.Nil, kv)
    }
  }

  def mget(mget: Mget, kv: KV):(Arrays, KV) = {
    val values = mget.keys.map(key => kv.get(key))
      .collect { case Some(StringValue(e)) => e }
      .map(BulkStrings.apply)
    (Arrays(values), kv)
  }

  // TODO: Implement TTL
  def set(set: Set, kv: KV):(SimpleString, KV) = {
    (Reply.Ok, kv + ((set.key, StringValue(set.value))))
  }

  def append(append: Append, kv: KV):(Integers, KV) = {
    val previous = kv.get(append.key).collect { case StringValue(str) => str}.getOrElse(ByteArrayWrapper.empty)
    val value = previous ++ append.value
    val kvUpdated = kv + ((append.key, StringValue(value)))
    (Integers(value.length), kvUpdated)
  }
}
