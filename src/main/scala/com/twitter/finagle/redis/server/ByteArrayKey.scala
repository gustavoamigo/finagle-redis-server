package com.twitter.finagle.redis.server

import java.util

/**
  * ByteArrayKey so equals and hashCode are not instance attached.
  * Note that Array[Byte] cannot be modified in order for this to work.
  * @param byteArray
  */
class ByteArrayKey(val byteArray: Array[Byte]) {

  private lazy val byteArrayHashCode =  util.Arrays.hashCode(byteArray)

  override def equals(other: Any): Boolean = other match {
    case that: ByteArrayKey =>
      util.Arrays.equals(byteArray, that.byteArray)
    case _ => false
  }

  override def hashCode(): Int = byteArrayHashCode

  def ++(other: ByteArrayKey) = Array.concat(byteArray,other.byteArray)
}

object ByteArrayKey {
  lazy val empty = new ByteArrayKey(Array.emptyByteArray)
}
