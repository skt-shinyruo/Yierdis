package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.TransactionState;

import java.util.List;
import java.util.Objects;

/**
 * Redis 事务命令（最小实现）：MULTI/EXEC/DISCARD。
 * <p>
 * 设计约束：
 * - 连接级状态通过 ServerSession 暴露，避免 core 依赖 server/Netty
 * - MULTI 态下，普通命令由 {@link YierdisFastCommandProcessor} 负责入队并返回 QUEUED
 */
final class TransactionCommands implements CommandModule {
    private final YierdisFastCommandProcessor processor;

    TransactionCommands(YierdisFastCommandProcessor processor) {
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("MULTI", CommandDescriptor.of(1, 0, 0, 0), CommandParsers.exactRequest(1, "multi"), this::multi);
        registration.register("DISCARD", CommandDescriptor.of(1, 0, 0, 0), CommandParsers.exactRequest(1, "discard"), this::discard);
        registration.register("EXEC", CommandDescriptor.of(1, 0, 0, 0), CommandParsers.exactRequest(1, "exec"), this::exec);
    }

    private void multi(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
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
        ReplyWriter out = ctx.out();
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
        ReplyWriter out = ctx.out();
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
                processor.execute(replay, ctx);
            }
        }
    }

    private TransactionState tx(CommandContext ctx) {
        return ctx.session().transaction();
    }

    private static void wrongArity(ReplyWriter out, String cmdLower) {
        out.error("ERR wrong number of arguments for '" + cmdLower + "' command");
    }
}
