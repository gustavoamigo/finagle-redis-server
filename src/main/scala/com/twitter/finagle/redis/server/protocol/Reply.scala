package com.twitter.finagle.redis.server.protocol

import com.twitter.finagle.redis.server.ByteArrayWrapper

sealed trait Reply {
  val EOL = "\r\n".getBytes
  def decode: List[Byte]
}

case class SimpleString(str: ByteArrayWrapper) extends Reply {
  override def decode: List[Byte] = {
    val startString = "+".getBytes
    List(startString, str: Array[Byte], EOL).flatten
  }
}

case class Error(msg: String) extends Reply {
  override def decode: List[Byte] = {
    val startString = "-ERR ".getBytes
    List(startString, msg.getBytes, EOL).flatten
  }
}

case class Integers(int: Long) extends Reply {
  override def decode: List[Byte] = {
    val startString = ":".getBytes
    List(startString, int.toString.getBytes, EOL).flatten
  }
}

case class BulkStrings(str: ByteArrayWrapper) extends Reply {
  override def decode: List[Byte] = {
    val startString = "$".getBytes
    if((str: Array[Byte]).isEmpty) {
      List(startString, "-1".getBytes, EOL).flatten
    } else {
      val size = str.length.toString.getBytes
      List(startString, size, EOL, str: Array[Byte], EOL).flatten
    }
  }
}

case class Arrays(values: List[Reply]) extends Reply {
  override def decode: List[Byte] = {
    val startString = "*".getBytes
    val firstLine = List(startString, values.length.toString.getBytes, EOL).flatten
    val others = values.flatMap(_.decode)
    List(firstLine, others).flatten
  }
}

object Reply {
  val Ok = SimpleString("OK".getBytes)
  val Nil = BulkStrings(Array.emptyByteArray)
}

