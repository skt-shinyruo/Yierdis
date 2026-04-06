package yier.bubu.redis;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.Objects;

// Server 连接状态根对象：作为 yierdis-server 中唯一的 Channel.attr(...) 所有者，统一管理 session/runtime/scheduling 三个切片。
final class ServerConnectionContext {
    private static final AttributeKey<ServerConnectionContext> KEY =
            AttributeKey.valueOf("yierdis.serverConnectionContext");

    static ServerConnectionContext getOrCreate(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        Attribute<ServerConnectionContext> attr = channel.attr(KEY);
        ServerConnectionContext existing = attr.get();
        if (existing != null) {
            return existing;
        }

        ServerConnectionContext created = new ServerConnectionContext();
        ServerConnectionContext raced = attr.setIfAbsent(created);
        return raced == null ? created : raced;
    }

    static ServerConnectionContext getOrCreate(
            Channel channel,
            int transactionQueueMaxCommands,
            long transactionQueueMaxBytes
    ) {
        ServerConnectionContext context = getOrCreate(channel);
        context.configureSessionLimits(transactionQueueMaxCommands, transactionQueueMaxBytes);
        return context;
    }

    private final ServerRuntimeState runtime;
    private final NettyExecutorChannelState scheduling;
    private volatile ServerSessionState session;
    private int transactionQueueMaxCommands = ServerSessionState.DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS;
    private long transactionQueueMaxBytes = ServerSessionState.DEFAULT_TRANSACTION_QUEUE_MAX_BYTES;
    private boolean sessionLimitsConfigured;

    private ServerConnectionContext() {
        this.runtime = new ServerRuntimeState();
        this.scheduling = new NettyExecutorChannelState();
    }

    ServerSessionState commandSession() {
        ServerSessionState existing = session;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (session == null) {
                session = new ServerSessionState(runtime, transactionQueueMaxCommands, transactionQueueMaxBytes);
            }
            return session;
        }
    }

    NettyExecutorChannelState queueState() {
        return scheduling;
    }

    boolean isClosing() {
        return runtime.isClosing();
    }

    boolean markClosing() {
        boolean changed = !runtime.isClosing();
        runtime.markClosing(commandSession());
        return changed;
    }

    void recordCommandEnqueued(int retainedBytes) {
        runtime.pendingCounter().incrementAndGet();
        runtime.pendingBytesCounter().addAndGet(Math.max(0, retainedBytes));
        runtime.commandsEnqueuedCounter().incrementAndGet();
    }

    void recordCommandRejected() {
        runtime.commandsRejectedCounter().incrementAndGet();
    }

    void recordCommandFinished(int retainedBytes, boolean executed) {
        if (executed) {
            runtime.commandsExecutedCounter().incrementAndGet();
        }
        runtime.pendingCounter().decrementAndGet();
        runtime.pendingBytesCounter().addAndGet(-Math.max(0, retainedBytes));
    }

    void recordSkippedClosing() {
        runtime.commandsSkippedClosingCounter().incrementAndGet();
    }

    void recordCloseAfterReply() {
        runtime.closeAfterReplyCounter().incrementAndGet();
    }

    void recordBackpressureEnter() {
        runtime.backpressureEnterCounter().incrementAndGet();
    }

    void recordBackpressureExit() {
        runtime.backpressureExitCounter().incrementAndGet();
    }

    int pending() {
        return runtime.pendingCounter().get();
    }

    long pendingBytes() {
        return runtime.pendingBytesCounter().get();
    }

    boolean markAutoReadDisabledByExecutor() {
        return runtime.markAutoReadDisabledByExecutor();
    }

    boolean autoReadDisabledByExecutor() {
        return runtime.autoReadDisabledByExecutor();
    }

    boolean clearAutoReadDisabledByExecutor() {
        return runtime.clearAutoReadDisabledByExecutor();
    }

    ConnectionStatsSnapshot statsSnapshot() {
        return new ConnectionStatsSnapshot(
                runtime.pendingCounter().get(),
                runtime.pendingBytesCounter().get(),
                runtime.autoReadDisabledByExecutor(),
                runtime.isClosing(),
                runtime.commandsEnqueuedCounter().get(),
                runtime.commandsExecutedCounter().get(),
                runtime.commandsRejectedCounter().get(),
                runtime.commandsSkippedClosingCounter().get(),
                runtime.closeAfterReplyCounter().get(),
                runtime.backpressureEnterCounter().get(),
                runtime.backpressureExitCounter().get()
        );
    }

    record ConnectionStatsSnapshot(
            int pending,
            long pendingBytes,
            boolean autoReadDisabledByExecutor,
            boolean closing,
            long commandsEnqueued,
            long commandsExecuted,
            long commandsRejected,
            long commandsSkippedClosing,
            long closeAfterReply,
            long backpressureEnter,
            long backpressureExit
    ) {
    }

    // Compatibility accessors kept for non-Task3 call sites/tests; behavior should use APIs above.
    ServerSessionState session() {
        return commandSession();
    }

    ServerRuntimeState runtime() {
        return runtime;
    }

    NettyExecutorChannelState scheduling() {
        return queueState();
    }

    private synchronized void configureSessionLimits(int transactionQueueMaxCommands, long transactionQueueMaxBytes) {
        if (session != null || sessionLimitsConfigured) {
            return;
        }
        this.transactionQueueMaxCommands = Math.max(0, transactionQueueMaxCommands);
        this.transactionQueueMaxBytes = Math.max(0, transactionQueueMaxBytes);
        this.sessionLimitsConfigured = true;
    }
}
