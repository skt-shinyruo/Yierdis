package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ProtocolNegotiationSession;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;
import yier.bubu.redis.execution.api.Session;

import java.util.Objects;

public final class RespReplyWriterFactory implements ReplyWriterFactory {
    @Override
    public ReplyWriter newWriter(BytesSink out) {
        return newWriter(null, out);
    }

    @Override
    public ReplyWriter newWriter(Session session, BytesSink out) {
        if (!(session instanceof ProtocolNegotiationSession protocolSession)) {
            return new RespReplyWriter(Objects.requireNonNull(out, "out"), RespProtocolVersion.RESP2);
        }
        return new RespReplyWriter(Objects.requireNonNull(out, "out"), protocolSession::respVersion);
    }
}
