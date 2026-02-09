package yier.bubu.redis.protocol.netty;

import java.util.Objects;

/**
 * Custom Protocol v1 的协议错误事件（decoder 输出，供 pipeline 上层统一编码回包）。
 * <p>
 * 约束：decoder 只负责 framing/parse/resync，不直接写回 NDJSON reply。
 */
public record ProtocolError(String message) {
    public ProtocolError {
        message = Objects.requireNonNullElse(message, "Protocol error");
    }
}

