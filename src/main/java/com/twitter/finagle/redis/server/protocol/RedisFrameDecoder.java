package com.twitter.finagle.redis.server.protocol;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

import java.io.ByteArrayOutputStream;
import java.util.LinkedList;
import java.util.List;

public class RedisFrameDecoder extends ReplayingDecoder<RedisFrameDecoder.State> {

    public RedisFrameDecoder() {
        super(State.READ_ARRAY_LENGTH, true);
    }

    private long arrayLength = 0;
    private long stringLength = 0;
    private List<byte[]> lines = new LinkedList<>();

    enum State {
        READ_ARRAY_LENGTH, READ_STRING_LENGTH, READ_CONTENT;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, State state) throws Exception {
        switch(state) {
            case READ_ARRAY_LENGTH:
                {
                    lines.clear();
                    arrayLength = 0;
                    byte[] line = readLine(buffer);
                    switch (line[0]) {
                        case '*':
                            String numStr = new String(line, 1, line.length - 1);
                            arrayLength = Long.valueOf(numStr);
                            checkpoint(State.READ_STRING_LENGTH);
                            return null;
                        default:
                            lines.add(line);
                            checkpoint(State.READ_ARRAY_LENGTH);
                            return linesToChannelBuffer(lines);
                    }
                }
            case READ_STRING_LENGTH:
                {
                    byte[] line = readLine(buffer);
                    if(line[0] != '$') throw new Error("Expceted '$'");
                    String numStr = new String(line, 1, line.length - 1);
                    stringLength = Long.valueOf(numStr);
                    checkpoint(State.READ_CONTENT);
                    return null;
                }

            case READ_CONTENT:
                byte[] line = readContent(buffer, stringLength);
                lines.add(line);
                arrayLength--;
                if(arrayLength > 0) {
                    checkpoint(State.READ_STRING_LENGTH);
                    return null;
                } else {
                    checkpoint(State.READ_ARRAY_LENGTH);
                    return linesToChannelBuffer(lines);
                }
            default:
                throw new Error("Shouldn't reach here.");
        }
    }

    private ChannelBuffer linesToChannelBuffer(List<byte[]> lines) {
        byte[] SPC = {' '};
        ChannelBuffer channelBuffer = ChannelBuffers.dynamicBuffer();
        for(byte[] line: lines) {
            channelBuffer.writeBytes(line);
            channelBuffer.writeBytes(SPC);
        }
        return channelBuffer;
    }

    private byte[] readLine(ChannelBuffer buffer) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while(true) {
            byte b = buffer.readByte();
            if (b == '\r') {
                continue;
            } else if (b == '\n' ) {
                break;
            }
            output.write(b);
        }
        return output.toByteArray();
    }

    private byte[] readContent(ChannelBuffer buffer, long length) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while(length > 0) {
            byte b = buffer.readByte();
            output.write(b);
            length --;
        }
        buffer.skipBytes(2);
        return output.toByteArray();
    }
}
