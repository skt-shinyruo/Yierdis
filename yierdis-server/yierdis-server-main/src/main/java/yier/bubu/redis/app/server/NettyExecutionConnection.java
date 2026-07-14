package yier.bubu.redis.app.server;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import yier.bubu.redis.execution.engine.EngineSession;
import yier.bubu.redis.execution.executor.ExecutionConnection;
import yier.bubu.redis.execution.executor.ExecutionConnectionContext;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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

        ExecutionConnectionContext context = new ExecutionConnectionContext();
        EngineSession session = new EngineSession(txMaxCommands, txMaxBytes);
        session.bindConnectionStatsSupplier(context::statsSnapshot);
        NettyExecutionConnection created = new NettyExecutionConnection(
                channel,
                session,
                context
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
    private final EngineSession session;
    private final ExecutionConnectionContext context;
    private volatile NettyReplyDecodedMessageGate replyGate;

    private NettyExecutionConnection(Channel channel, EngineSession session, ExecutionConnectionContext context) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.session = Objects.requireNonNull(session, "session");
        this.context = Objects.requireNonNull(context, "context");
    }

    Channel channel() {
        return channel;
    }

    void bindReplyGate(NettyReplyDecodedMessageGate replyGate) {
        this.replyGate = Objects.requireNonNull(replyGate, "replyGate");
    }

    NettyReplyDecodedMessageGate replyGate() {
        return replyGate;
    }

    CompletableFuture<Void> shutdownReplyGracefully() {
        NettyReplyDecodedMessageGate gate = replyGate;
        if (gate != null) {
            return gate.shutdownGracefully();
        }

        CompletableFuture<Void> closed = new CompletableFuture<>();
        channel.closeFuture().addListener(future -> {
            if (future.isSuccess()) {
                closed.complete(null);
            } else {
                closed.completeExceptionally(future.cause());
            }
        });
        channel.close();
        return closed;
    }

    @Override
    public String connectionId() {
        return channel.id().asShortText();
    }

    @Override
    public EngineSession session() {
        return session;
    }

    @Override
    public ExecutionConnectionContext context() {
        return context;
    }

    @Override
    public boolean markClosing() {
        if (!context.markClosing()) {
            return false;
        }
        session.discardTransaction();
        return true;
    }
}
