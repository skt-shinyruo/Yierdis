package yier.bubu.redis.command.defaults;

// BulkStringReplyAdapter：将 core 的 BulkStringSink 适配到命令层 Redis reply writer，避免 DB 层关心 RESP bytes。

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.execution.api.ReplyWriter;

import java.util.Objects;

public final class BulkStringReplyAdapter implements BulkStringSink {
    private final ReplyWriter out;

    public BulkStringReplyAdapter(ReplyWriter out) {
        this.out = Objects.requireNonNull(out, "out");
    }

    @Override
    public void bulkString(byte[] data) {
        out.bulkString(data);
    }

    @Override
    public void bulkString(byte[] data, int off, int len) {
        out.bulkString(data, off, len);
    }

    @Override
    public void bulkString(BytesSlice slice) {
        out.bulkString(slice);
    }

    @Override
    public void bulkStringLongAscii(long value) {
        out.bulkStringLongAscii(value);
    }
}
