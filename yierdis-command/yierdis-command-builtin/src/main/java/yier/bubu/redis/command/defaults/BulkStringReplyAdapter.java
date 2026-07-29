package yier.bubu.redis.command.defaults;

// 将 storage 字节值适配到语义回复使用的流式 ReplySink，不承担协议头或资源关闭。

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.execution.api.ReplySink;

import java.util.Objects;

public final class BulkStringReplyAdapter implements ByteValueSink {
    private final ReplySink reply;

    public BulkStringReplyAdapter(ReplySink reply) {
        this.reply = Objects.requireNonNull(reply, "reply");
    }

    @Override
    public void value(byte[] data) {
        reply.bulkString(data);
    }

    @Override
    public void value(byte[] data, int off, int len) {
        reply.bulkString(data, off, len);
    }

    @Override
    public void value(BytesSlice slice) {
        reply.bulkString(slice);
    }

    @Override
    public void longAscii(long value) {
        reply.bulkStringLongAscii(value);
    }

    @Override
    public void nullValue() {
        reply.bulkStringNull();
    }
}
