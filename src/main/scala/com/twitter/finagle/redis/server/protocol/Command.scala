package com.twitter.finagle.redis.server.protocol

sealed trait Command

case class Get(key: Array[Byte]) extends Command
case class Set(key: Array[Byte], value: Array[Byte], ex: Option[Int], px: Option[Int], nx: Boolean, xx: Boolean) extends Command
case class Error(err: String) extends Command



