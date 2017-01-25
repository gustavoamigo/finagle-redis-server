package com.twitter.finagle.redis.server.protocol

import java.util

import com.twitter.finagle.redis.server.ByteArrayWrapper
import com.twitter.io.Buf

object CommandParser {

  def apply(buffers: List[Buf]): Command = {
    val parts = buffers match {
      case head :: Nil =>
        (new String(Buf.ByteArray.Owned.extract(head)), List.empty)
      case head :: tail =>
        (new String(Buf.ByteArray.Owned.extract(head)), tail.map(Buf.ByteArray.Owned.extract))
      case _ => ("", List.empty)
    }
    parts match {
      case ("PING", args) if args.size <= 1 =>
        if(args.size == 1) {
          Ping(Some(args(0)))
        } else {
          Ping(None)
        }
      case ("GET", args) if args.size == 1 =>
        Get(args(0))
      case ("MGET", args) =>
        Mget(args.map(ba => new ByteArrayWrapper(ba)))
      case ("SET", args) if args.size >= 2 =>
        Set(
          key = args(0),
          value = args(1),
          nx = parseArgBoolean(args, "NX"),
          xx = parseArgBoolean(args, "XX"),
          ex = parseArgKeyValue(args, "EX"),
          px = parseArgKeyValue(args, "PX")
        )
      case ("APPEND", args) if args.size == 2 =>
        Append(
          key = args(0),
          value = args(1)
        )
      case error =>
        println(s"unknown command")
        Unknown(s"unknown command")
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
