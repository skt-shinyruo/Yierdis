package yier.bubu.redis.execution.api;

import java.util.Objects;
import yier.bubu.redis.common.command.MutationContext;

/**
 * 容量预留成功后的一次命令执行作用域。
 */
public final class CommandExecutionContext implements AutoCloseable {
    private final CommandSession session;
    private final RedisReplyWriter reply;
    private MutationContext mutationContext;

    private CommandExecutionContext(
            CommandSession session,
            RedisReplyWriter reply,
            MutationContext mutationContext
    ) {
        this.session = Objects.requireNonNull(session, "session");
        this.reply = Objects.requireNonNull(reply, "reply");
        this.mutationContext = Objects.requireNonNull(mutationContext, "mutationContext");
    }

    public static CommandExecutionContext forRequest(
            CommandSession session,
            RedisReplyWriter reply,
            ExecutionRequest request
    ) {
        return new CommandExecutionContext(
                session,
                reply,
                MutationContext.of(Objects.requireNonNull(request, "request"))
        );
    }

    public CommandSession session() {
        return session;
    }

    public RedisReplyWriter reply() {
        return reply;
    }

    public MutationContext mutationContext() {
        return mutationContext;
    }

    @Override
    public void close() {
        MutationContext owned = mutationContext;
        if (owned == null) {
            return;
        }
        mutationContext = null;
        owned.close();
    }
}
