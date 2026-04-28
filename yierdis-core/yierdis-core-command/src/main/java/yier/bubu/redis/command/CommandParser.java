package yier.bubu.redis.command;

@FunctionalInterface
interface CommandParser<T> {
    CommandParseResult<T> parse(ArgReader args);
}
