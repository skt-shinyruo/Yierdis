package yier.bubu.redis.client;

import java.util.Objects;

/**
 * 顶层 {@code -} 回复。{@link #getMessage()} 是服务端原文。当前回复已经读完，连接可以继续用。
 */
public class ServerException extends RuntimeException {
    public ServerException(String serverText) {
        super(Objects.requireNonNull(serverText, "serverText"));
    }
}
