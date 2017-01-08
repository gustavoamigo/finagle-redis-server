package gustavoamigo.redis

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.redis.server.protocol.BufServerPipelineFactory
import com.twitter.finagle.{Codec, Service}
import com.twitter.util.Future
import com.twitter.io.Buf
import gustavoamigo.redis.store.KeyValueStore


object RedisServer extends App {
  val kvStore = new KeyValueStore[String, Array[Byte]]
  val codec = Codec.ofPipelineFactory[Buf,Buf](BufServerPipelineFactory.getPipeline)
  val server = ServerBuilder()
    .codec(codec)
    .bindTo(new InetSocketAddress(8080))
    .name("redisServer").build(new RedisService(kvStore))

}
