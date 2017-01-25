package com.twitter.finagle.redis.server.protocol

import com.twitter.finagle.redis.server.ByteArrayWrapper

sealed trait Resp {
  val EOL = "\r\n".getBytes
  def decode: Array[Byte]
}

case class SimpleStringResp(str: ByteArrayWrapper) extends Resp {
  override def decode: Array[Byte] = {
    val startString = "+".getBytes
    Array(startString, str: Array[Byte], EOL).flatten
  }
}

case class ErrorResp(msg: String) extends Resp {
  override def decode: Array[Byte] = {
    val startString = "-ERR ".getBytes
    Array(startString, msg.getBytes, EOL).flatten
  }
}

case class IntegerResp(int: Long) extends Resp {
  override def decode: Array[Byte] = {
    val startString = ":".getBytes
    Array(startString, int.toString.getBytes, EOL).flatten
  }
}

case class BulkStringResp(str: ByteArrayWrapper) extends Resp {
  override def decode: Array[Byte] = {
    val startString = "$".getBytes
    if((str: Array[Byte]).isEmpty) {
      Array(startString, "-1".getBytes, EOL).flatten
    } else {
      val size = str.length.toString.getBytes
      Array(startString, size, EOL, str: Array[Byte], EOL).flatten
    }
  }
}

case class ArrayResp(values: List[Resp]) extends Resp {
  override def decode: Array[Byte] = {
    val startString = "*".getBytes
    val firstLine = Array(startString, values.length.toString.getBytes, EOL).flatten
    val others = values.toArray.flatMap(_.decode)
    Array(firstLine, others).flatten
  }
}

object Resp {
  val Ok = SimpleStringResp("OK".getBytes)
  val Nil = BulkStringResp(Array.emptyByteArray)
}

