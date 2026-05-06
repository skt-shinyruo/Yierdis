package yier.bubu.redis.command.api;

import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Objects;
import java.util.function.Function;

public final class CommandParsers {
    private CommandParsers() {
    }

    public static CommandParser<ArgReader> passThrough() {
        return args -> CommandParseResult.ok(args);
    }

    public static CommandParser<ArgReader> exact(int argc, String commandLower) {
        return arity(CommandArity.exact(argc, commandLower));
    }

    public static CommandParser<ArgReader> min(int minArgc, String commandLower) {
        return arity(CommandArity.min(minArgc, commandLower));
    }

    public static CommandParser<ArgReader> range(int minArgc, int maxArgc, String commandLower) {
        return arity(CommandArity.range(minArgc, maxArgc, commandLower));
    }

    public static CommandParser<ArgReader> oneOf(String commandLower, int... allowedArgc) {
        return arity(CommandArity.oneOf(commandLower, allowedArgc));
    }

    public static CommandParser<ArgReader> pairTail(int minArgc, int tailStartIndex, String commandLower) {
        return arity(CommandArity.pairTail(minArgc, tailStartIndex, commandLower));
    }

    public static CommandParser<ExecutionRequest> exactRequest(int argc, String commandLower) {
        return request(CommandArity.exact(argc, commandLower));
    }

    public static CommandParser<ExecutionRequest> minRequest(int minArgc, String commandLower) {
        return request(CommandArity.min(minArgc, commandLower));
    }

    public static CommandParser<ExecutionRequest> rangeRequest(int minArgc, int maxArgc, String commandLower) {
        return request(CommandArity.range(minArgc, maxArgc, commandLower));
    }

    public static CommandParser<ExecutionRequest> oneOfRequest(String commandLower, int... allowedArgc) {
        return request(CommandArity.oneOf(commandLower, allowedArgc));
    }

    public static CommandParser<ExecutionRequest> request(CommandArity arity) {
        return arity(arity, ArgReader::request);
    }

    public static <T> CommandParser<T> arity(CommandArity arity, Function<ArgReader, T> mapper) {
        Objects.requireNonNull(arity, "arity");
        Objects.requireNonNull(mapper, "mapper");
        return args -> {
            CommandParseError error = arity.validate(args);
            if (error != null) {
                return CommandParseResult.error(error);
            }
            return CommandParseResult.ok(mapper.apply(args));
        };
    }

    private static CommandParser<ArgReader> arity(CommandArity arity) {
        return arity(arity, Function.identity());
    }
}
