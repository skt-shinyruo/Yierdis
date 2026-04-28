package yier.bubu.redis.command;

import java.util.Objects;
import java.util.function.Function;

final class CommandParsers {
    private CommandParsers() {
    }

    static CommandParser<ArgReader> passThrough() {
        return args -> CommandParseResult.ok(args);
    }

    static CommandParser<ArgReader> exact(int argc, String commandLower) {
        return arity(CommandArity.exact(argc, commandLower));
    }

    static CommandParser<ArgReader> min(int minArgc, String commandLower) {
        return arity(CommandArity.min(minArgc, commandLower));
    }

    static CommandParser<ArgReader> range(int minArgc, int maxArgc, String commandLower) {
        return arity(CommandArity.range(minArgc, maxArgc, commandLower));
    }

    static CommandParser<ArgReader> oneOf(String commandLower, int... allowedArgc) {
        return arity(CommandArity.oneOf(commandLower, allowedArgc));
    }

    static CommandParser<ArgReader> pairTail(int minArgc, int tailStartIndex, String commandLower) {
        return arity(CommandArity.pairTail(minArgc, tailStartIndex, commandLower));
    }

    static <T> CommandParser<T> arity(CommandArity arity, Function<ArgReader, T> mapper) {
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
