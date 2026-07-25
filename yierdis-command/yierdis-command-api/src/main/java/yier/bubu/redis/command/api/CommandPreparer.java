package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.PreparedCommand;

@FunctionalInterface
public interface CommandPreparer<T> {
    PreparedCommand prepare(T parsed, CommandPreparationContext context);
}
