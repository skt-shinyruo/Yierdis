package yier.bubu.redis.command.api;

import java.util.Objects;

public final class CommandParseResult<T> {
    private final T value;
    private final CommandParseError error;

    private CommandParseResult(T value, CommandParseError error) {
        this.value = value;
        this.error = error;
    }

    public static <T> CommandParseResult<T> ok(T value) {
        return new CommandParseResult<>(Objects.requireNonNull(value, "value"), null);
    }

    public static <T> CommandParseResult<T> error(CommandParseError error) {
        return new CommandParseResult<>(null, Objects.requireNonNull(error, "error"));
    }

    public boolean ok() {
        return error == null;
    }

    public T value() {
        return value;
    }

    public CommandParseError error() {
        return error;
    }
}
