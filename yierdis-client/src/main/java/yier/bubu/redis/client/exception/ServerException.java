package yier.bubu.redis.client.exception;

import java.util.Objects;

/**
 * 服务端 {@code -} 回复，既可作为顶层异常抛出，也可作为 EXEC 结果列表中的单条错误。
 * {@link #getMessage()} 是服务端原文。当前回复已经读完，连接可以继续用。
 */
public class ServerException extends RuntimeException {
    public ServerException(String serverText) {
        super(Objects.requireNonNull(serverText, "serverText"));
    }
}
