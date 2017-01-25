package com.twitter.finagle.redis.server.protocol

import com.twitter.finagle.Failure
import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.io.Buf
import com.twitter.io.Buf.ByteArray
import org.jboss.netty.channel.{ChannelHandlerContext, Channels, MessageEvent, SimpleChannelHandler}

import scala.collection.JavaConversions._

class RedisCodec extends SimpleChannelHandler {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
    e.getMessage match {
      case b: Buf => Channels.write(ctx, e.getFuture, BufChannelBuffer(b))
      case typ => e.getFuture.setFailure(Failure(
        s"unexpected type ${typ.getClass.getSimpleName} when encoding to ChannelBuffer"))
    }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
    e.getMessage match {
      case frame: RedisFrame =>
        val message: List[Buf] = frame.getParts.toList.map(ByteArray.Owned.apply)
        Channels.fireMessageReceived(ctx, message)
      case typ => Channels.fireExceptionCaught(ctx, Failure(
        s"unexpected type ${typ.getClass.getSimpleName} when encoding to Buf"))
    }
}
