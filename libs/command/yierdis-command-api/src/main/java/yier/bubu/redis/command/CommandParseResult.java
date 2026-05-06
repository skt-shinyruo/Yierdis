package yier.bubu.redis.command;

import java.util.Objects;

final class CommandParseResult<T> {
    private final T value;
    private final CommandParseError error;

    private CommandParseResult(T value, CommandParseError error) {
        this.value = value;
        this.error = error;
    }

    static <T> CommandParseResult<T> ok(T value) {
        return new CommandParseResult<>(Objects.requireNonNull(value, "value"), null);
    }

    static <T> CommandParseResult<T> error(CommandParseError error) {
        return new CommandParseResult<>(null, Objects.requireNonNull(error, "error"));
    }

    boolean ok() {
        return error == null;
    }

    T value() {
        return value;
    }

    CommandParseError error() {
        return error;
    }
}
