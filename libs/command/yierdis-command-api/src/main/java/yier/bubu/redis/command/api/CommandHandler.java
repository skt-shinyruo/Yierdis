package yier.bubu.redis.command.api;

import yier.bubu.redis.contract.CommandContext;

@FunctionalInterface
public interface CommandHandler<T> {
    void execute(T args, CommandContext ctx);
}
