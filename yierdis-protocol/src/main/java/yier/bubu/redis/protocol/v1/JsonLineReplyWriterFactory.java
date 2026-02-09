package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.protocol.ReplyWriter;
import yier.bubu.redis.protocol.ReplyWriterFactory;
import yier.bubu.redis.protocol.Session;

import java.util.Objects;

/**
 * Default {@link ReplyWriterFactory} for Custom Protocol v1 (JSON line replies).
 */
public final class JsonLineReplyWriterFactory implements ReplyWriterFactory {
    @Override
    public ReplyWriter newWriter(BytesSink out, Session session) {
        Objects.requireNonNull(out, "out");
        return new JsonLineReplyWriter(out, session);
    }
}

