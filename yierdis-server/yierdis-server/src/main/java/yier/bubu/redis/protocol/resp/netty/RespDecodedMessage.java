package yier.bubu.redis.protocol.resp.netty;

import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;

/**
 * RESP 解码完成后、进入 execution ingress 前的封闭消息类型。
 */
public sealed interface RespDecodedMessage extends AutoCloseable
        permits RespDecodedMessage.Request, RespProtocolError {
    record Request(ExecutionRequest request) implements RespDecodedMessage {
        public Request {
            Objects.requireNonNull(request, "request");
        }
    }

    @Override
    default void close() {
        switch (this) {
            case Request value -> value.request.close();
            case RespProtocolError ignored -> {
            }
        }
    }
}
