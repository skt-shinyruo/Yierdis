package yier.bubu.redis.execution.api;

import java.util.Objects;

public record CommandResult(RedisReply reply, boolean closeAfterReply) {
    public CommandResult {
        Objects.requireNonNull(reply, "reply");
    }

    public static CommandResult reply(RedisReply reply) {
        return new CommandResult(reply, false);
    }

    public static CommandResult error(String message) {
        return reply(RedisReplies.error(message));
    }

    public static CommandResult controlError(String message) {
        return reply(RedisReplies.controlError(message));
    }

    public static CommandResult closeAfterReply(RedisReply reply) {
        return new CommandResult(reply, true);
    }
}
