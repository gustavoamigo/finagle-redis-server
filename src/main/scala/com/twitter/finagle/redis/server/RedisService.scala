package com.twitter.finagle.redis.server

import com.twitter.finagle.Service
import com.twitter.finagle.redis.server.runner.CommandRunner
import com.twitter.io.Buf
import com.twitter.util.Future
import com.twitter.concurrent.AsyncMutex
import com.twitter.finagle.redis.server.protocol.{Command, CommandParser}
import com.twitter.io.Buf.ByteArray

class RedisService extends Service[Buf, Buf] {
  private var kv: CommandRunner.KV = Map.empty
  private val mutex = new AsyncMutex()
  override def apply(request: Buf): Future[Buf] = {
    val parsedCmd: Command = CommandParser(request)
    val sync = mutex.acquireAndRunSync {
      val (reply, newKv) = CommandRunner.run(parsedCmd, kv)
      kv = newKv
      reply
    }
    sync.map(reply => ByteArray.apply(reply.decode :_*))
  }
}




