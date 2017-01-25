package com.twitter.finagle.redis.server.protocol


import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}

object RedisServerPipelineFactory extends ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("redisFrameDecoder", new RedisFrameDecoder )
      pipeline.addLast("redisCodec", new RedisCodec)
      pipeline
    }
}