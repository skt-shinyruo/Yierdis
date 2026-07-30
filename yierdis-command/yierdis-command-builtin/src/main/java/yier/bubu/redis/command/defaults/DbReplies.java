package yier.bubu.redis.command.defaults;

import java.util.Objects;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteValue;

public final class DbReplies {
    private DbReplies() {
    }

    public static RedisReply value(ByteValue value) {
        ByteValue source = Objects.requireNonNull(value, "value");
        if (source.isNull()) {
            return RedisReplies.nullValue();
        }
        return RedisReplies.bulkString(
                source.payloadLength(),
                source.retainedMemoryBytes(),
                sink -> source.emitTo(new BulkStringReplyAdapter(sink)));
    }

    public static RedisReply sequence(ByteSequenceSource source) {
        ByteSequenceSource values = Objects.requireNonNull(source, "source");
        return RedisReplies.sequence(
                values.elementCount(),
                values.retainedMemoryBytes(),
                consumer -> values.visitElementLengths(consumer::accept),
                sink -> values.emitTo(new BulkStringReplyAdapter(sink)));
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
                sink -> values.emitTo(new BulkStringReplyAdapter(sink)));
    }

    public static RedisReply map(ByteMapSource source) {
        ByteMapSource values = Objects.requireNonNull(source, "source");
        return RedisReplies.byteMap(
                values.pairCount(),
                values.retainedMemoryBytes(),
                consumer -> values.visitPairLengths(consumer::accept),
                sink -> values.emitPairsTo(new BulkStringReplyAdapter(sink)));
    }
}
