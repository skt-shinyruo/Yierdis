package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.Objects;

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

    public static RedisReply booleanValue(boolean value) {
        return new RedisReply.BooleanValue(value);
    }

    public static RedisReply doubleValue(double value) {
        return new RedisReply.DoubleValue(value);
    }

    public static RedisReply bigNumber(String ascii) {
        return new RedisReply.BigNumber(ascii);
    }

    public static RedisReply verbatimString(String format, byte[] data) {
        return new RedisReply.VerbatimString(format, data);
    }

    public static RedisReply blobError(String message) {
        return new RedisReply.BlobError(message);
    }

    public static RedisReply bulkString(byte[] data) {
        byte[] captured = Objects.requireNonNull(data, "data").clone();
        return bulkString(captured.length, 0L, sink -> sink.bulkString(captured));
    }

    public static RedisReply bulkString(
            int payloadLength,
            long retainedSourceBytes,
            RedisReply.PayloadEmitter emitter
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
        return aggregate(ReplyShape.AggregateKind.ARRAY, elements);
    }

    public static RedisReply map(List<RedisReply> fieldValues) {
        return aggregate(ReplyShape.AggregateKind.MAP, fieldValues);
    }

    public static RedisReply set(List<RedisReply> elements) {
        return aggregate(ReplyShape.AggregateKind.SET, elements);
    }

    public static RedisReply push(List<RedisReply> elements) {
        return aggregate(ReplyShape.AggregateKind.PUSH, elements);
    }

    public static RedisReply attribute(List<RedisReply> fieldValues) {
        return aggregate(ReplyShape.AggregateKind.ATTRIBUTE, fieldValues);
    }

    public static RedisReply sequence(
            int elementCount,
            long retainedSourceBytes,
            ReplyShape.PayloadLengths payloadLengths,
            RedisReply.PayloadEmitter emitter
    ) {
        return new RedisReply.ByteSequence(
                elementCount, retainedSourceBytes, payloadLengths, emitter);
    }

    public static RedisReply byteSet(
            int elementCount,
            long retainedSourceBytes,
            ReplyShape.PayloadLengths payloadLengths,
            RedisReply.PayloadEmitter emitter
    ) {
        return new RedisReply.ByteSet(elementCount, retainedSourceBytes, payloadLengths, emitter);
    }

    public static RedisReply byteMap(
            int pairCount,
            long retainedSourceBytes,
            ReplyShape.PayloadLengths payloadLengths,
            RedisReply.PayloadEmitter emitter
    ) {
        return new RedisReply.ByteMap(pairCount, retainedSourceBytes, payloadLengths, emitter);
    }

    private static RedisReply aggregate(
            ReplyShape.AggregateKind kind,
            List<RedisReply> elements
    ) {
        return new RedisReply.Aggregate(kind, elements);
    }
}
