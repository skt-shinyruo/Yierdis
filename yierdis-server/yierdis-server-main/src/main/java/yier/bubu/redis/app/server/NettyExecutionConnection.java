package yier.bubu.redis.app.server;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import yier.bubu.redis.execution.engine.EngineSession;
import yier.bubu.redis.execution.executor.ExecutionConnection;
import yier.bubu.redis.execution.executor.ExecutionConnectionContext;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
    private volatile Consumer<Runnable> ownerTaskExecutor = Runnable::run;
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

    void bindOwnerTaskExecutor(Consumer<Runnable> ownerTaskExecutor) {
        this.ownerTaskExecutor = Objects.requireNonNull(ownerTaskExecutor, "ownerTaskExecutor");
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
        Runnable discard = session::discardTransaction;
        try {
            // MULTI 队列由 command owner 回收，避免 transport event loop 与命令执行并发释放请求。
            ownerTaskExecutor.accept(discard);
        } catch (Throwable schedulingFailure) {
            // owner 已退出时仍要归还请求引用；transaction 自身同步保证兜底清理不会重复释放。
            discard.run();
        }
        return true;
    }
}
