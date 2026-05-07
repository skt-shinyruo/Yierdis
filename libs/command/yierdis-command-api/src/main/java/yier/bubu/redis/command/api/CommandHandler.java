package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.CommandContext;

@FunctionalInterface
public interface CommandHandler<T> {
    void execute(T args, CommandContext ctx);
}
