package com.twitter.finagle.redis.server.protocol


import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}

object BufServerPipelineFactory extends ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("frameDecoder", new RedisFrameDecoder )
      pipeline.addLast("endec", new RedisCodec)
      pipeline
    }
}