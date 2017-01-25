package com.twitter.finagle.redis.server.protocol

import com.twitter.finagle.redis.server.ByteArrayWrapper

sealed trait Resp {
  val EOL = "\r\n".getBytes
  def decode: List[Byte]
}

case class SimpleStringResp(str: ByteArrayWrapper) extends Resp {
  override def decode: List[Byte] = {
    val startString = "+".getBytes
    List(startString, str: Array[Byte], EOL).flatten
  }
}

case class ErrorResp(msg: String) extends Resp {
  override def decode: List[Byte] = {
    val startString = "-ERR ".getBytes
    List(startString, msg.getBytes, EOL).flatten
  }
}

case class IntegerResp(int: Long) extends Resp {
  override def decode: List[Byte] = {
    val startString = ":".getBytes
    List(startString, int.toString.getBytes, EOL).flatten
  }
}

case class BulkStringResp(str: ByteArrayWrapper) extends Resp {
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

case class ArrayResp(values: List[Resp]) extends Resp {
  override def decode: List[Byte] = {
    val startString = "*".getBytes
    val firstLine = List(startString, values.length.toString.getBytes, EOL).flatten
    val others = values.flatMap(_.decode)
    List(firstLine, others).flatten
  }
}

object Resp {
  val Ok = SimpleStringResp("OK".getBytes)
  val Nil = BulkStringResp(Array.emptyByteArray)
}

