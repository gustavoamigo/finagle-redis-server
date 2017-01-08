package com.twitter.finagle.redis.server.protocol

import com.twitter.io.Buf

import scala.annotation.tailrec
object CommandParser {

  @tailrec
  def extractParts(bytes: Array[Byte], acc: List[Array[Byte]] = List.empty): List[Array[Byte]] = {
    if(bytes.length == 0) {
      acc.reverse
    } else {
      val (quoted,pos) = partEndPos(bytes)
      val start = if(quoted) 1 else 0
      val part = bytes.slice(start, pos)
      val tail = bytes.slice(pos + 1, bytes.length)
      extractParts(tail, part :: acc)
    }
  }

  private def partEndPos(bytes: Array[Byte]): (Boolean, Int) = {
    bytes(0).toChar match {
      case '"' =>
        var i = 1
        while(i < bytes.length) {
          if(bytes(i).toChar == '"')
            return (true,i)
          i += 1
        }
        (true,i)
      case _ =>
        var i = 0
        while(i < bytes.length) {
          if(bytes(i).toChar == ' ')
            return (false,i)
          i += 1
        }
        (false,i)
    }
  }

  object CommandParts {
    def unapply(bytes: Array[Byte]): Option[(String, List[Array[Byte]])] = {
      bytes.apply(0).toChar match {
        case '-'|'+'|':'|'$'|'*' => None
        case _ =>
          val parts = extractParts(bytes)
          Some(new String(parts.head), parts.tail)
      }
    }
  }

  private def parseArgKeyValue(args: List[Array[Byte]], argKey: String): Option[Int] = {
    val pos = args.indexOf("EX".getBytes)
    if(pos != -1) {
      Some(Integer.valueOf(new String(args(pos + 1))))
    } else {
      None
    }
  }

  def apply(buf: Buf): Command = {
    val bytes = Buf.ByteArray.Owned.extract(buf)
    bytes match {
      case CommandParts("GET", args) =>
        Get(args(0))
      case CommandParts("SET", args) if args.size >= 2 =>
        Set(
          key = args(0),
          value = args(1),
          nx = args.contains("NX".getBytes),
          xx = args.contains("XX".getBytes),
          ex = parseArgKeyValue(args, "EX"),
          px = parseArgKeyValue(args, "PX")
        )
      case _ =>
        Error("Command not found")

    }
  }

}