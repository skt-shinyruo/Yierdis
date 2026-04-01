package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ReplyWriterFactory;
import yier.bubu.redis.executor.ExecutorBacklogBudget;
import yier.bubu.redis.executor.ExecutorBackpressureController;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class NettyCommandExecutionSupport {
    private final Logger log;
    private final YierdisFastCommandProcessor commandProcessor;
    private final ReplyWriterFactory replyWriterFactory;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorBackpressureController<Channel> backpressureController;
    private final int backpressureLowWatermark;
    private final long backpressureBytesHighWatermark;
    private final long backpressureBytesLowWatermark;
    private final BooleanSupplier running;
    private final LongAdder commandsExecuted;
    private final LongAdder commandsSkippedClosing;
    private final LongAdder closeAfterReply;
    private CommandContext execCtx;

    NettyCommandExecutionSupport(
            Logger log,
            YierdisFastCommandProcessor commandProcessor,
            ReplyWriterFactory replyWriterFactory,
            ExecutorBacklogBudget backlogBudget,
            ExecutorBackpressureController<Channel> backpressureController,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            BooleanSupplier running,
            LongAdder commandsExecuted,
            LongAdder commandsSkippedClosing,
            LongAdder closeAfterReply
    ) {
        this.log = Objects.requireNonNull(log, "log");
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.backpressureController = Objects.requireNonNull(backpressureController, "backpressureController");
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.running = Objects.requireNonNull(running, "running");
        this.commandsExecuted = Objects.requireNonNull(commandsExecuted, "commandsExecuted");
        this.commandsSkippedClosing = Objects.requireNonNull(commandsSkippedClosing, "commandsSkippedClosing");
        this.closeAfterReply = Objects.requireNonNull(closeAfterReply, "closeAfterReply");
    }

    ReplyWriter newReplyWriter(ByteBuf out, Channel ch) {
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(ch, "ch");
        return replyWriterFactory.newWriter(new NettyByteBufSink(out));
    }

    void executeCommand(Command cmd, Channel ch, ReplyWriter writer) {
        Objects.requireNonNull(cmd, "cmd");
        Objects.requireNonNull(ch, "ch");
        Objects.requireNonNull(writer, "writer");
        ServerSessionState session = ServerConnectionContext.getOrCreate(ch).session();
        commandProcessor.execute(cmd, context(session, writer));
    }

    void recordSkippedClosing(Channel ch) {
        if (ch == null) {
            return;
        }
        ServerRuntimeState conn = ServerConnectionContext.getOrCreate(ch).runtime();
        conn.commandsSkippedClosingCounter().incrementAndGet();
        commandsSkippedClosing.increment();
    }

    void markCloseAfterReply(Channel ch) {
        if (ch == null) {
            return;
        }
        ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
        ServerRuntimeState conn = context.runtime();
        ServerSessionState session = context.session();
        conn.closeAfterReplyCounter().incrementAndGet();
        closeAfterReply.increment();
        conn.markClosing(session);
        backpressureController.disableAutoRead(ch);
    }

    void handleExecutionFailure(ChannelHandlerContext ctx, Channel ch, Throwable t) {
        try {
            if (ch != null) {
                ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
                context.runtime().markClosing(context.session());
                backpressureController.disableAutoRead(ch);
            }
        } catch (Throwable ignored) {
            // ignore
        }
        try {
            if (ctx != null && ch != null) {
                ByteBuf out = ctx.alloc().buffer();
                boolean ok = false;
                try {
                    ReplyWriter writer = newReplyWriter(out, ch);
                    writer.internalError("ERR internal error");
                    ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
                    ok = true;
                } finally {
                    if (!ok) {
                        out.release();
                    }
                }
            }
        } catch (Throwable ignored) {
            // ignore
        }
        log.debug("Command execution failed", t);
    }

    void onCommandFinished(Channel ch, int retainedBytes, boolean executed) {
        ServerRuntimeState conn = ServerConnectionContext.getOrCreate(ch).runtime();
        if (executed) {
            conn.commandsExecutedCounter().incrementAndGet();
            commandsExecuted.increment();
        }

        int now = conn.pendingCounter().decrementAndGet();
        long bytesNow = conn.pendingBytesCounter().addAndGet(-retainedBytes);
        backlogBudget.releaseQueuedBytes(retainedBytes);
        backlogBudget.releaseSlot();

        boolean pendingOk = now <= backpressureLowWatermark;
        boolean bytesOk = backpressureBytesHighWatermark <= 0 || bytesNow <= backpressureBytesLowWatermark;
        boolean globalOk = backlogBudget.isGlobalBackpressureCleared();
        if (running.getAsBoolean() && !isChannelClosing(ch) && pendingOk && bytesOk && globalOk) {
            backpressureController.enableAutoReadIfWeDisabled(ch);
        }
        if (running.getAsBoolean() && globalOk) {
            backpressureController.scheduleGlobalRecovery();
        }
    }

    void recycleAndRelease(NettyExecutorTask task) {
        if (task == null || task.cmd == null) {
            return;
        }
        try {
            task.cmd.close();
        } catch (Throwable ignored) {
            // ignore
        }
        if (task.ctx != null) {
            onCommandFinished(task.ctx.channel(), task.retainedBytes, false);
            return;
        }
        backlogBudget.releaseQueuedBytes(task.retainedBytes);
        backlogBudget.releaseSlot();
    }

    static boolean isChannelClosing(Channel ch) {
        if (ch == null) {
            return false;
        }
        return ServerConnectionContext.getOrCreate(ch).runtime().isClosing();
    }

    static int safeRetainedBytes(Command cmd) {
        if (cmd == null) {
            return 0;
        }
        try {
            return Math.max(0, cmd.retainedBytes());
        } catch (Throwable ignored) {
            return 0;
        }
    }

    private CommandContext context(ServerSessionState session, ReplyWriter out) {
        CommandContext ctx = execCtx;
        if (ctx == null) {
            ctx = new CommandContext(session, out);
            execCtx = ctx;
            return ctx;
        }
        return ctx.reset(session, out);
    }
}
