package yier.bubu.redis.command.api;

@FunctionalInterface
public interface CommandParser<T> {
    CommandParseResult<T> parse(ArgReader args);
}
