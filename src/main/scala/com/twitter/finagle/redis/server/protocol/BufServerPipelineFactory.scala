package com.twitter.finagle.redis.server.protocol

import com.twitter.finagle.netty3.codec.BufCodec
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.frame.{DelimiterBasedFrameDecoder, Delimiters}

object BufServerPipelineFactory extends ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("frameDecoder", new RedisFrameDecoder )
      pipeline.addLast("endec", new BufCodec)
      pipeline
    }
}