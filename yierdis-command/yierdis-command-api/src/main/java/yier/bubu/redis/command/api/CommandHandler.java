package yier.bubu.redis.command.api;

@FunctionalInterface
public interface CommandHandler {
    CommandInvocation parse(CommandArgs args) throws CommandParseException;
}
