package yier.bubu.redis.command;

@FunctionalInterface
public interface CommandParser<T> {
    CommandParseResult<T> parse(ArgReader args);
}
