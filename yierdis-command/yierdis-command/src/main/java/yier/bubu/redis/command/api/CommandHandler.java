package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;

import java.util.function.Function;

@FunctionalInterface
public interface CommandHandler {
    Function<CommandSession, PreparedCommand> parse(CommandArgs args);
}
