package yier.bubu.redis.client.exception;

/**
 * 连接没建立。读写失败、断连、无法重新对齐的帧、超过上限的 bulk 会关掉连接，并且不自动重试。
 * 已经读完的 RESP3 帧也用这个异常失败那一条命令，连接保持可用。
 * 等待事件循环接受写出时被打断同样抛出，这时连接不一定已关。
 */
public class ConnectionException extends RuntimeException {
    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
