package yier.bubu.redis.command.api;

public final class CommandParsers {
    private CommandParsers() {
    }

    public static CommandParser<ArgReader> args() {
        return CommandParseResult::ok;
    }
}
