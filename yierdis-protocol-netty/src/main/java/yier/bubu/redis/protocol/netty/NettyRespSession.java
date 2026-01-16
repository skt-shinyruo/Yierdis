package yier.bubu.redis.protocol.netty;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespSession;

import java.util.Objects;

/**
 * Netty-backed {@link RespSession} that stores connection-level protocol state in {@link Channel} attributes.
 */
public final class NettyRespSession implements RespSession {
    private static final AttributeKey<RespProtocol> PROTOCOL =
            AttributeKey.valueOf("yierdis.resp.protocol");

    private final Channel channel;

    public NettyRespSession(Channel channel) {
        this.channel = Objects.requireNonNull(channel, "channel");
    }

    @Override
    public RespProtocol protocol() {
        RespProtocol p = channel.attr(PROTOCOL).get();
        return p == null ? RespProtocol.RESP2 : p;
    }

    @Override
    public void setProtocol(RespProtocol protocol) {
        channel.attr(PROTOCOL).set(protocol == null ? RespProtocol.RESP2 : protocol);
    }
}
