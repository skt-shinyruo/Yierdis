package yier.bubu.redis.execution.api;

import java.util.Objects;

/**
 * 命令准备阶段可读取的连接会话。
 */
public record CommandPreparationContext(CommandSession session) {
    public CommandPreparationContext {
        Objects.requireNonNull(session, "session");
    }
}
