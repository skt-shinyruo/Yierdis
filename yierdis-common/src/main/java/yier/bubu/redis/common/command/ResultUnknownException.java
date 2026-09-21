package yier.bubu.redis.common.command;

public class ResultUnknownException extends RuntimeException {
    public ResultUnknownException(String message, Throwable cause) {
        super(message, cause);
    }
}
