package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;

import java.util.Objects;

public final class RespReplyWriterFactory implements ReplyWriterFactory {
    @Override
    public ReplyWriter newWriter(BytesSink out) {
        return new RespReplyWriter(Objects.requireNonNull(out, "out"), RespProtocolVersion.RESP2);
    }
}
