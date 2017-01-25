package com.twitter.finagle.redis.server

import java.net.InetSocketAddress

import com.twitter.finagle.Codec
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.redis.server.protocol.RedisServerPipelineFactory
import com.twitter.io.Buf

object RedisServer extends App {
  val codec = Codec.ofPipelineFactory[List[Buf],Buf](RedisServerPipelineFactory.getPipeline)
  val server = ServerBuilder()
    .codec(codec)
    .bindTo(new InetSocketAddress(8080))
    .name("redisServer").build(new RedisService())
}
