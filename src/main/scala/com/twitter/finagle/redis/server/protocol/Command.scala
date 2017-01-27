package com.twitter.finagle.redis.server.protocol

import com.twitter.finagle.redis.server.ByteArrayKey

sealed trait Command

case class Ping(value: Option[Array[Byte]]) extends Command

// String
case class Get(key: ByteArrayKey) extends Command
case class Mget(keys: List[ByteArrayKey]) extends Command
case class Set(key: ByteArrayKey, value: Array[Byte], ex: Option[Int], px: Option[Int], nx: Boolean, xx: Boolean) extends Command
case class Append(key: ByteArrayKey, value: Array[Byte]) extends Command
case class Incr(key: ByteArrayKey) extends Command

//List
case class Lpush(list: ByteArrayKey, value: Array[Byte]) extends Command

case class Unknown(err: String) extends Command



