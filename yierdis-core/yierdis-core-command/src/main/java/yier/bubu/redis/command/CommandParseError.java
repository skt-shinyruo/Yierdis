package yier.bubu.redis.command;

import java.util.Objects;

final class CommandParseError {
    private enum Kind {
        WRONG_ARITY,
        SYNTAX,
        INTEGER_OUT_OF_RANGE,
        CUSTOM
    }

    private final Kind kind;
    private final String commandLower;
    private final String message;

    private CommandParseError(Kind kind, String commandLower, String message) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.commandLower = commandLower;
        this.message = message;
    }

    static CommandParseError wrongArity(String commandLower) {
        if (commandLower == null || commandLower.isBlank()) {
            throw new IllegalArgumentException("commandLower must not be blank");
        }
        return new CommandParseError(Kind.WRONG_ARITY, commandLower, null);
    }

    static CommandParseError syntax() {
        return new CommandParseError(Kind.SYNTAX, null, null);
    }

    static CommandParseError integerOutOfRange() {
        return new CommandParseError(Kind.INTEGER_OUT_OF_RANGE, null, null);
    }

    static CommandParseError custom(String message) {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be blank");
        }
        return new CommandParseError(Kind.CUSTOM, null, message);
    }

    String toReplyMessage() {
        return switch (kind) {
            case WRONG_ARITY -> "ERR wrong number of arguments for '" + commandLower + "' command";
            case SYNTAX -> "ERR syntax error";
            case INTEGER_OUT_OF_RANGE -> "ERR value is not an integer or out of range";
            case CUSTOM -> message;
        };
    }
}
