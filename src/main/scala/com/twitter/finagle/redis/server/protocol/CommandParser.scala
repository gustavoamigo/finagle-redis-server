package com.twitter.finagle.redis.server.protocol

import java.util

import com.twitter.finagle.redis.server.ByteArrayWrapper
import com.twitter.io.Buf

import scala.annotation.tailrec
object CommandParser {

  def apply(buf: Buf): Command = {
    val bytes = Buf.ByteArray.Owned.extract(buf)
    bytes match {
      case CommandParts("GET", args) if args.size == 1 =>
        Get(args(0))
      case CommandParts("MGET", args) =>
        Mget(args.map(ba => new ByteArrayWrapper(ba)))
      case CommandParts("SET", args) if args.size >= 2 =>
        Set(
          key = args(0),
          value = args(1),
          nx = parseArgBoolean(args, "NX"),
          xx = parseArgBoolean(args, "XX"),
          ex = parseArgKeyValue(args, "EX"),
          px = parseArgKeyValue(args, "PX")
        )
      case CommandParts("APPEND", args) if args.size == 2 =>
        Append(
          key = args(0),
          value = args(1)
        )
      case error =>
        Unknown(s"unknown command '${new String(error)}'")
    }
  }

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

  private object CommandParts {
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
    val pos = args.indexWhere(arr => util.Arrays.equals(arr, argKey.getBytes))
    if(pos != -1) {
      Some(Integer.valueOf(new String(args(pos + 1))))
    } else {
      None
    }
  }

  private def parseArgBoolean(args: List[Array[Byte]], argKey: String) =
    args.exists(arr => util.Arrays.equals(arr, argKey.getBytes))



}
