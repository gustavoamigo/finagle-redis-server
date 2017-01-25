package com.twitter.finagle.redis.server.protocol

import com.twitter.finagle.redis.server.ByteArrayWrapper

sealed trait Command

case class Ping(value: Option[ByteArrayWrapper]) extends Command

case class Get(key: ByteArrayWrapper) extends Command
case class Mget(keys: List[ByteArrayWrapper]) extends Command
case class Set(key: ByteArrayWrapper, value: ByteArrayWrapper, ex: Option[Int], px: Option[Int], nx: Boolean, xx: Boolean) extends Command
case class Append(key: ByteArrayWrapper, value: ByteArrayWrapper) extends Command

case class Unknown(err: String) extends Command



