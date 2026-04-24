package yier.bubu.redis;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import yier.bubu.redis.executor.DefaultExecutionSession;
import yier.bubu.redis.executor.ExecutionConnection;
import yier.bubu.redis.executor.ExecutionConnectionContext;

import java.util.Objects;

final class NettyExecutionConnection implements ExecutionConnection {
    private static final AttributeKey<NettyExecutionConnection> KEY =
            AttributeKey.valueOf("yierdis.nettyExecutionConnection");

    static NettyExecutionConnection getOrCreate(Channel channel, int txMaxCommands, long txMaxBytes) {
        Objects.requireNonNull(channel, "channel");
        Attribute<NettyExecutionConnection> attr = channel.attr(KEY);
        NettyExecutionConnection existing = attr.get();
        if (existing != null) {
            return existing;
        }

        DefaultExecutionSession session = new DefaultExecutionSession(txMaxCommands, txMaxBytes);
        NettyExecutionConnection created = new NettyExecutionConnection(
                channel,
                new ExecutionConnectionContext(session)
        );
        NettyExecutionConnection raced = attr.setIfAbsent(created);
        return raced == null ? created : raced;
    }

    static NettyExecutionConnection get(Channel channel) {
        if (channel == null) {
            return null;
        }
        return channel.attr(KEY).get();
    }

    private final Channel channel;
    private final ExecutionConnectionContext context;

    private NettyExecutionConnection(Channel channel, ExecutionConnectionContext context) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.context = Objects.requireNonNull(context, "context");
    }

    Channel channel() {
        return channel;
    }

    @Override
    public String connectionId() {
        return channel.id().asShortText();
    }

    @Override
    public ExecutionConnectionContext context() {
        return context;
    }
}
