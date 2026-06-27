package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.List;
import java.util.Objects;

/**
 * Redis 事务命令（最小实现）：MULTI/EXEC/DISCARD。
 * <p>
 * 设计约束：
 * - 连接级事务状态通过 TransactionSession 暴露，避免 core 依赖 server/Netty
 * - MULTI 态下，普通命令由 {@link YierdisFastCommandProcessor} 负责入队并返回 QUEUED
 */
final class TransactionCommands implements CommandModule {
    private final QueuedCommandReplayer replayer;

    TransactionCommands(QueuedCommandReplayer replayer) {
        this.replayer = Objects.requireNonNull(replayer, "replayer");
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

        List<ExecutionRequest> queued = tx.drain();
        out.arrayHeader(queued.size());
        for (ExecutionRequest queuedRequest : queued) {
            try (ExecutionRequest replay = queuedRequest) {
                CommandContext replayCtx = new CommandContext(ctx.sessionCapabilities(), out);
                replayer.replay(replay, replayCtx);
            }
        }
    }

    private TransactionState tx(CommandContext ctx) {
        return ctx.transactionSession().transaction();
    }

    private static void wrongArity(RedisReplyWriter out, String cmdLower) {
        out.error("ERR wrong number of arguments for '" + cmdLower + "' command");
    }
}
