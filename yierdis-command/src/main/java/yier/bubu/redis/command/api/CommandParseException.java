package yier.bubu.redis.command.api;

public final class CommandParseException extends RuntimeException {
    public CommandParseException(String replyMessage) {
        super(java.util.Objects.requireNonNull(replyMessage, "replyMessage"));
    }
}
