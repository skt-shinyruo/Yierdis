package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;

import java.util.Objects;

public final class RespReplyWriterFactory implements RedisReplyWriterFactory {
    @Override
    public RedisReplyWriter newWriter(CommandSession session, BytesSink out) {
        Objects.requireNonNull(session, "session");
        return new RespReplyWriter(Objects.requireNonNull(out, "out"), session::respVersion);
    }
}
