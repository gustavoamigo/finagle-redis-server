package com.twitter.finagle.redis.server

import java.util

/**
  * ByteArrayWrapper so equals and hashCode are not instance attached.
  * Note that Array[Byte] cannot be modified in order for this to work.
  * @param byteArray
  */
class ByteArrayWrapper(val byteArray: Array[Byte]) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[ByteArrayWrapper]

  override def equals(other: Any): Boolean = other match {
    case that: ByteArrayWrapper =>
      util.Arrays.equals(byteArray, that.byteArray)
    case _ => false
  }

  override def hashCode(): Int = {
    util.Arrays.hashCode(byteArray)
  }

  def ++(other: ByteArrayWrapper) = Array.concat(byteArray,other.byteArray)
}

object ByteArrayWrapper {
  lazy val empty = new ByteArrayWrapper(Array.emptyByteArray)
}
