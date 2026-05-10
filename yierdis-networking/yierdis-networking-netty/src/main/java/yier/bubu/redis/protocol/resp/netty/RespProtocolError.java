package yier.bubu.redis.protocol.resp.netty;

public record RespProtocolError(String message, boolean closeAfterReply) {
    public RespProtocolError {
        if (message == null || message.isBlank()) {
            message = "ERR Protocol error";
        }
    }
}
