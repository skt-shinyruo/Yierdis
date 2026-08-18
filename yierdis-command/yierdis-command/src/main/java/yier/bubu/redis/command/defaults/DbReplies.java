package yier.bubu.redis.command.defaults;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplySink;
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.ByteValueSink;

public final class DbReplies {
    private DbReplies() {
    }

    public static RedisReply value(ByteValue value) {
        ByteValue source = Objects.requireNonNull(value, "value");
        if (source.payloadLength() < 0) {
            return RedisReplies.nullValue();
        }
        return RedisReplies.bulkString(
                source.payloadLength(),
                source.retainedMemoryBytes(),
                sink -> source.emitTo(new ReplyByteValueSink(sink)));
    }

    public static RedisReply sequence(ByteSequenceSource source) {
        ByteSequenceSource values = Objects.requireNonNull(source, "source");
        return RedisReplies.sequence(
                values.elementCount(),
                values.retainedMemoryBytes(),
                consumer -> values.visitElementLengths(consumer::accept),
                sink -> values.emitTo(new ReplyByteValueSink(sink)));
    }

    public static RedisReply set(ByteSequenceSource source) {
        ByteSequenceSource values = Objects.requireNonNull(source, "source");
        return RedisReplies.byteSet(
                values.elementCount(),
                values.retainedMemoryBytes(),
                consumer -> values.visitElementLengths(consumer::accept),
                sink -> values.emitTo(new ReplyByteValueSink(sink)));
    }

    public static RedisReply singleValue(ByteSequenceSource source) {
        ByteSequenceSource values = Objects.requireNonNull(source, "source");
        if (values.elementCount() != 1) {
            throw new IllegalArgumentException("single value source must contain one element");
        }
        int[] payloadLength = {-1};
        values.visitElementLengths(length -> payloadLength[0] = length);
        return RedisReplies.bulkString(
                payloadLength[0],
                values.retainedMemoryBytes(),
                sink -> values.emitTo(new ReplyByteValueSink(sink)));
    }

    public static RedisReply map(ByteMapSource source) {
        ByteMapSource values = Objects.requireNonNull(source, "source");
        return RedisReplies.byteMap(
                values.pairCount(),
                values.retainedMemoryBytes(),
                consumer -> values.visitPairLengths(consumer::accept),
                sink -> values.emitPairsTo(new ReplyByteValueSink(sink)));
    }

    private static final class ReplyByteValueSink implements ByteValueSink {
        private final ReplySink reply;

        private ReplyByteValueSink(ReplySink reply) {
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
}
