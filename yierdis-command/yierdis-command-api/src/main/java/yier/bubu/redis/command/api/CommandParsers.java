package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.ExecutionRequest;

public final class CommandParsers {
    private CommandParsers() {
    }

    public static CommandParser<ArgReader> args() {
        return CommandParseResult::ok;
    }

    public static CommandParser<ExecutionRequest> request() {
        return args -> CommandParseResult.ok(args.request());
    }
}
