package yier.bubu.redis.protocol.netty;

// 连接级上下文（协议会话 SSOT）：仅用于跟踪 RESP2/RESP3 协商状态（Netty adapter）。

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespSession;

import java.util.Objects;

/**
 * 连接级协议会话（SSOT）。
 * <p>
 * 该对象只承载协议协商状态（RESP2/RESP3），并通过 {@link Channel#attr(AttributeKey)} 与连接生命周期绑定。
 * <p>
 * 注意：server 侧的背压/统计/closing 等运行时连接状态属于服务端实现细节，应位于 server 模块中，
 * 避免 protocol adapter 携带 server 语义。
 */
public final class ConnectionContext implements RespSession {
    private static final AttributeKey<ConnectionContext> KEY =
            AttributeKey.valueOf("yierdis.connectionContext");

    public static ConnectionContext getOrCreate(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        Attribute<ConnectionContext> attr = channel.attr(KEY);
        ConnectionContext existing = attr.get();
        if (existing != null) {
            return existing;
        }
        ConnectionContext created = new ConnectionContext();
        ConnectionContext raced = attr.setIfAbsent(created);
        return raced == null ? created : raced;
    }

    // --- RESP session ---
    private volatile RespProtocol protocol = RespProtocol.RESP2;

    private ConnectionContext() {
    }

    @Override
    public RespProtocol protocol() {
        RespProtocol p = protocol;
        return p == null ? RespProtocol.RESP2 : p;
    }

    @Override
    public void setProtocol(RespProtocol protocol) {
        this.protocol = protocol == null ? RespProtocol.RESP2 : protocol;
    }
}
