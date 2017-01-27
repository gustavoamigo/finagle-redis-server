package com.twitter.finagle.redis.server

import com.twitter.finagle.Service
import com.twitter.finagle.redis.server.runner.{CommandRunner, Store}
import com.twitter.io.Buf
import com.twitter.util.Future
import com.twitter.concurrent.AsyncMutex
import com.twitter.finagle.redis.server.protocol.{Command, CommandParser}
import com.twitter.io.Buf.ByteArray

class RedisService extends Service[List[Buf], Buf] {
  private var store: Store = Store()
  private val mutex = new AsyncMutex()
  override def apply(request: List[Buf]): Future[Buf] = {
    val parsedCmd: Command = CommandParser(request)
    val sync = mutex.acquireAndRunSync {
      val (reply, newStore) = CommandRunner.run(parsedCmd, store)
      store = newStore
      reply
    }
    sync.map(reply => ByteArray.Owned(reply.decode))
  }
}