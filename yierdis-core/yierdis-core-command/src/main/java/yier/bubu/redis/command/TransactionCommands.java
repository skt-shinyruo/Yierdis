package yier.bubu.redis.command;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;

import java.util.List;
import java.util.Objects;

/**
 * Redis 事务命令（最小实现）：MULTI/EXEC/DISCARD。
 * <p>
 * 设计约束：
 * - 连接级状态通过 {@link ServerSession} 暴露，避免 core 依赖 server/Netty
 * - MULTI 态下，普通命令由 {@link YierdisFastCommandProcessor} 负责入队并返回 QUEUED
 */
final class TransactionCommands implements CommandModule {
    private final CommandSupport support;
    private final YierdisFastCommandProcessor processor;

    TransactionCommands(CommandSupport support, YierdisFastCommandProcessor processor) {
        this.support = Objects.requireNonNull(support, "support");
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("MULTI", this::multi, CommandDescriptor.of(1, 0, 0, 0));
        registration.register("DISCARD", this::discard, CommandDescriptor.of(1, 0, 0, 0));
        registration.register("EXEC", this::exec, CommandDescriptor.of(1, 0, 0, 0));
    }

    private void multi(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 1) {
            CommandSupport.wrongArity(out, "multi");
            return;
        }
        TransactionState tx = txOrNull(ctx);
        if (tx == null) {
            out.error("ERR MULTI is only supported on server connections");
            return;
        }
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
            CommandSupport.wrongArity(out, "discard");
            return;
        }
        TransactionState tx = txOrNull(ctx);
        if (tx == null || !tx.active()) {
            out.error("ERR DISCARD without MULTI");
            return;
        }
        tx.discard();
        out.simpleString("OK");
    }

    private void exec(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 1) {
            CommandSupport.wrongArity(out, "exec");
            return;
        }
        TransactionState tx = txOrNull(ctx);
        if (tx == null || !tx.active()) {
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

    private TransactionState txOrNull(CommandContext ctx) {
        ServerSession s = ctx.serverSessionOrNull();
        if (s != null) {
            return s.transaction();
        }
        return null;
    }
}
