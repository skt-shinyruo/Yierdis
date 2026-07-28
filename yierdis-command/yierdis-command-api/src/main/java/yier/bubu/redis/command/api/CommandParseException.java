package yier.bubu.redis.command.api;

import java.util.Objects;

public final class CommandParseException extends Exception {
    private final String replyMessage;

    public CommandParseException(String replyMessage) {
        super(Objects.requireNonNull(replyMessage, "replyMessage"));
        this.replyMessage = replyMessage;
    }

    public String replyMessage() {
        return replyMessage;
    }
}
