package yier.bubu.redis.execution.api;

import java.util.Objects;
import yier.bubu.redis.common.command.MutationContext;

/**
 * 容量预留成功后的一次命令执行作用域。
 */
public final class CommandExecutionContext implements AutoCloseable {
    private final CommandSession session;
    private MutationContext mutationContext;

    private CommandExecutionContext(
            CommandSession session,
            MutationContext mutationContext
    ) {
        this.session = Objects.requireNonNull(session, "session");
        this.mutationContext = Objects.requireNonNull(mutationContext, "mutationContext");
    }

    public static CommandExecutionContext forRequest(
            CommandSession session,
            ExecutionRequest request
    ) {
        return new CommandExecutionContext(
                session,
                MutationContext.of(Objects.requireNonNull(request, "request"))
        );
    }

    public CommandSession session() {
        return session;
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
