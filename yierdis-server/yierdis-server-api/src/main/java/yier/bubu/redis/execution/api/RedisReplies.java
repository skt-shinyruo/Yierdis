package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

public final class RedisReplies {
    private RedisReplies() {
    }

    public static RedisReply simpleString(String value) {
        return new RedisReply.SimpleString(value);
    }

    public static RedisReply error(String message) {
        return new RedisReply.Error(message);
    }

    public static RedisReply controlError(String message) {
        return new RedisReply.ControlError(message);
    }

    public static RedisReply integer(long value) {
        return new RedisReply.IntegerValue(value);
    }

    public static RedisReply bulkString(byte[] data) {
        byte[] captured = Objects.requireNonNull(data, "data").clone();
        return bulkString(captured.length, 0L, sink -> sink.bulkString(captured));
    }

    public static RedisReply bulkString(
            int payloadLength,
            long retainedSourceBytes,
            Consumer<ReplySink> emitter
    ) {
        return new RedisReply.BulkString(payloadLength, retainedSourceBytes, emitter);
    }

    public static RedisReply nullValue() {
        return new RedisReply.NullValue();
    }

    public static RedisReply nullArray() {
        return new RedisReply.NullArray();
    }

    public static RedisReply array(List<RedisReply> elements) {
        return new RedisReply.Aggregate(ReplyShape.AggregateKind.ARRAY, elements);
    }

    public static RedisReply map(List<RedisReply> fieldValues) {
        return new RedisReply.Aggregate(ReplyShape.AggregateKind.MAP, fieldValues);
    }

    public static RedisReply sequence(
            int elementCount,
            long retainedSourceBytes,
            Consumer<IntConsumer> payloadLengths,
            Consumer<ReplySink> emitter
    ) {
        return new RedisReply.ByteSequence(
                elementCount, retainedSourceBytes, payloadLengths, emitter);
    }

    public static RedisReply byteSet(
            int elementCount,
            long retainedSourceBytes,
            Consumer<IntConsumer> payloadLengths,
            Consumer<ReplySink> emitter
    ) {
        return new RedisReply.ByteSet(elementCount, retainedSourceBytes, payloadLengths, emitter);
    }

    public static RedisReply byteMap(
            int pairCount,
            long retainedSourceBytes,
            Consumer<IntConsumer> payloadLengths,
            Consumer<ReplySink> emitter
    ) {
        return new RedisReply.ByteMap(pairCount, retainedSourceBytes, payloadLengths, emitter);
    }
}
