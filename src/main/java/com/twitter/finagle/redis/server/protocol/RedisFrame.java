package com.twitter.finagle.redis.server.protocol;

import java.util.List;

public class RedisFrame {
    private List<byte[]> parts;

    public RedisFrame(List<byte[]> parts) {
        this.parts = parts;
    }

    public List<byte[]> getParts() {
        return parts;
    }
}
