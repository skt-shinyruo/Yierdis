package yier.bubu.redis.storage.api;

import yier.bubu.redis.common.command.ResultUnknownException;

public final class PostCommitMutationException extends ResultUnknownException {
    public PostCommitMutationException(String message, Throwable cause) {
        super(message, cause);
    }
}
