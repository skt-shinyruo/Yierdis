package yier.bubu.redis.execution.api;

import java.util.Objects;

/**
 * 一次命令执行作用域。
 */
public final class CommandExecutionContext {
    private final CommandSession session;

    private CommandExecutionContext(CommandSession session) {
        this.session = Objects.requireNonNull(session, "session");
    }

    public static CommandExecutionContext forSession(CommandSession session) {
        return new CommandExecutionContext(session);
    }

    public CommandSession session() {
        return session;
    }
}
