package yier.bubu.redis.protocol.custom.v1.execution;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;

import java.util.Objects;

/**
 * Default {@link ReplyWriterFactory} for Custom Protocol v1 (JSON line replies).
 */
public final class JsonLineReplyWriterFactory implements ReplyWriterFactory {
    @Override
    public ReplyWriter newWriter(BytesSink out) {
        Objects.requireNonNull(out, "out");
        return new JsonLineReplyWriter(out);
    }
}
