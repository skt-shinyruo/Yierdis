package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;
import yier.bubu.redis.execution.api.ServerSession;

import java.util.Objects;

public final class RespReplyWriterFactory implements ReplyWriterFactory {
    @Override
    public ReplyWriter newWriter(BytesSink out) {
        return newWriter(null, out);
    }

    @Override
    public ReplyWriter newWriter(ServerSession session, BytesSink out) {
        if (session == null) {
            return new RespReplyWriter(Objects.requireNonNull(out, "out"), RespProtocolVersion.RESP2);
        }
        return new RespReplyWriter(Objects.requireNonNull(out, "out"), session::respVersion);
    }
}
