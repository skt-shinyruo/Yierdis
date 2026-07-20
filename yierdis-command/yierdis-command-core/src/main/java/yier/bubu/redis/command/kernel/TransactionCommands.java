package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Redis 事务命令（最小实现）：MULTI/EXEC/DISCARD。
 * <p>
 * 设计约束：
 * - 连接级事务状态通过 TransactionSession 暴露，避免 core 依赖 server/Netty
 * - MULTI 态下，普通命令由 {@link YierdisFastCommandProcessor} 负责入队并返回 QUEUED
 */
final class TransactionCommands implements CommandModule {
    private final QueuedCommandReplayer replayer;
    private final Function<? super ExecutionRequest, ReplyPlan> replyPlanner;

    TransactionCommands(QueuedCommandReplayer replayer) {
        this(replayer, ignored -> ReplyPlan.maximum());
    }

    TransactionCommands(
            QueuedCommandReplayer replayer,
            Function<? super ExecutionRequest, ReplyPlan> replyPlanner
    ) {
        this.replayer = Objects.requireNonNull(replayer, "replayer");
        this.replyPlanner = Objects.requireNonNull(replyPlanner, "replyPlanner");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("MULTI", CommandDescriptor.of(1, 0, 0, 0), CommandParsers.exactRequest(1, "multi"), this::multi);
        registration.register("DISCARD", CommandDescriptor.of(1, 0, 0, 0), CommandParsers.exactRequest(1, "discard"), this::discard);
        registration.register("EXEC", CommandDescriptor.of(1, 0, 0, 0), CommandParsers.exactRequest(1, "exec"), this::exec);
    }

    private void multi(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 1) {
            wrongArity(out, "multi");
            return;
        }
        TransactionState tx = tx(ctx);
        if (tx.active()) {
            out.error("ERR MULTI calls can not be nested");
            return;
        }
        tx.begin();
        out.simpleString("OK");
    }

    private void discard(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 1) {
            wrongArity(out, "discard");
            return;
        }
        TransactionState tx = tx(ctx);
        if (!tx.active()) {
            out.error("ERR DISCARD without MULTI");
            return;
        }
        tx.discard();
        out.simpleString("OK");
    }

    private void exec(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 1) {
            wrongArity(out, "exec");
            return;
        }
        TransactionState tx = tx(ctx);
        if (!tx.active()) {
            out.error("ERR EXEC without MULTI");
            return;
        }
        if (tx.aborted()) {
            tx.discard();
            out.error("EXECABORT Transaction discarded because of previous errors.");
            return;
        }

        // envelope 必须在 drain 之前完成；容量不足时 executor 才能安全重试同一个 EXEC。
        out.requireReplyEnvelope(tx.planExecReply(replyPlanner));

        List<ExecutionRequest> queued = tx.drain();
        int nextOwnedIndex = 0;
        Throwable replayFailure = null;
        try {
            out.arrayHeader(queued.size());
            while (nextOwnedIndex < queued.size()) {
                ExecutionRequest replay = queued.get(nextOwnedIndex);
                // 先推进所有权游标；当前请求交给 try-with-resources，finally 只回收尚未开始 replay 的队尾。
                nextOwnedIndex++;
                try (replay) {
                    CommandContext replayCtx = new CommandContext(
                            ctx.sessionCapabilities(),
                            out,
                            MutationContext.of(replay)
                    );
                    try {
                        replayer.replay(replay, replayCtx);
                    } finally {
                        // TransactionCommands 创建 replay context，也负责结束借用；processor 的释放保持幂等。
                        replayCtx.releaseMutationContext();
                    }
                }
            }
        } catch (RuntimeException | Error failure) {
            replayFailure = failure;
            throw failure;
        } finally {
            closeRemainingRequests(queued, nextOwnedIndex, replayFailure);
        }
    }

    private static void closeRemainingRequests(
            List<ExecutionRequest> queued,
            int fromIndex,
            Throwable replayFailure
    ) {
        Throwable cleanupFailure = null;
        for (int index = fromIndex; index < queued.size(); index++) {
            try {
                queued.get(index).close();
            } catch (RuntimeException | Error failure) {
                if (cleanupFailure == null) {
                    cleanupFailure = failure;
                } else {
                    cleanupFailure.addSuppressed(failure);
                }
            }
        }
        if (cleanupFailure == null) {
            return;
        }
        if (replayFailure != null) {
            replayFailure.addSuppressed(cleanupFailure);
            return;
        }
        if (cleanupFailure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        throw (Error) cleanupFailure;
    }

    private TransactionState tx(CommandContext ctx) {
        return ctx.transactionSession().transaction();
    }

    private static void wrongArity(RedisReplyWriter out, String cmdLower) {
        out.error("ERR wrong number of arguments for '" + cmdLower + "' command");
    }
}
