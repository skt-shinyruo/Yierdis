package yier.bubu.redis.protocol;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * Connection-level RESP protocol version.
 * <p>
 * Default is RESP2; a client may switch to RESP3 using {@code HELLO 3}.
 */
public enum RespProtocol {
    RESP2(2),
    RESP3(3);

    public static final AttributeKey<RespProtocol> CHANNEL_PROTOCOL =
            AttributeKey.valueOf("yierdis.resp.protocol");

    private final int proto;

    RespProtocol(int proto) {
        this.proto = proto;
    }

    public int proto() {
        return proto;
    }

    public static RespProtocol get(Channel channel) {
        if (channel == null) {
            return RESP2;
        }
        RespProtocol p = channel.attr(CHANNEL_PROTOCOL).get();
        return p == null ? RESP2 : p;
    }

    public static void set(Channel channel, RespProtocol protocol) {
        if (channel == null) {
            return;
        }
        channel.attr(CHANNEL_PROTOCOL).set(protocol == null ? RESP2 : protocol);
    }
}

