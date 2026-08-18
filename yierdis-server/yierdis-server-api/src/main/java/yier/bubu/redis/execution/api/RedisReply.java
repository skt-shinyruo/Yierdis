package yier.bubu.redis.execution.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

public sealed interface RedisReply permits
        RedisReply.SimpleString, RedisReply.Error, RedisReply.ControlError,
        RedisReply.IntegerValue, RedisReply.BooleanValue, RedisReply.DoubleValue,
        RedisReply.BigNumber, RedisReply.VerbatimString, RedisReply.BlobError,
        RedisReply.BulkString, RedisReply.NullValue, RedisReply.NullArray,
        RedisReply.Aggregate, RedisReply.ByteSequence, RedisReply.ByteSet, RedisReply.ByteMap {

    default ReplyShape shape() {
        return switch (this) {
            case SimpleString value -> ReplyShapes.simpleString(value.value);
            case Error value -> ReplyShapes.error(value.message);
            case ControlError ignored -> ReplyShapes.maximum();
            case IntegerValue value -> ReplyShapes.integer(value.value);
            case BooleanValue value -> ReplyShapes.booleanValue(value.value);
            case DoubleValue value -> ReplyShapes.doubleValue(value.value);
            case BigNumber value -> ReplyShapes.bigNumber(value.ascii);
            case VerbatimString value -> ReplyShapes.verbatimString(value.format, value.data.length);
            case BlobError value -> ReplyShapes.blobError(value.message);
            case BulkString value -> ReplyShapes.bulkString(value.payloadLength, value.retainedSourceBytes);
            case NullValue ignored -> ReplyShapes.nullValue();
            case NullArray ignored -> ReplyShapes.nullArray();
            case Aggregate value -> aggregateShape(value);
            case ByteSequence value -> ReplyShapes.sequence(
                    value.elementCount, value.retainedSourceBytes, value.payloadLengths);
            case ByteSet value -> ReplyShapes.byteSet(
                    value.elementCount, value.retainedSourceBytes, value.payloadLengths);
            case ByteMap value -> ReplyShapes.byteMap(
                    value.pairCount, value.retainedSourceBytes, value.payloadLengths);
        };
    }

    record SimpleString(String value) implements RedisReply {
    }

    record Error(String message) implements RedisReply {
    }

    record ControlError(String message) implements RedisReply {
    }

    record IntegerValue(long value) implements RedisReply {
    }

    record BooleanValue(boolean value) implements RedisReply {
    }

    record DoubleValue(double value) implements RedisReply {
    }

    record BigNumber(String ascii) implements RedisReply {
    }

    record VerbatimString(String format, byte[] data) implements RedisReply {
        public VerbatimString {
            data = Objects.requireNonNull(data, "data").clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }

    record BlobError(String message) implements RedisReply {
    }

    record BulkString(
            int payloadLength,
            long retainedSourceBytes,
            Consumer<ReplySink> emitter
    ) implements RedisReply {
        public BulkString {
            requireNonNegative(payloadLength, "payloadLength");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
            Objects.requireNonNull(emitter, "emitter");
        }
    }

    record NullValue() implements RedisReply {
    }

    record NullArray() implements RedisReply {
    }

    record Aggregate(ReplyShape.AggregateKind kind, List<RedisReply> elements) implements RedisReply {
        public Aggregate {
            kind = Objects.requireNonNull(kind, "kind");
            elements = copyAggregateElements(kind, elements);
        }
    }

    record ByteSequence(
            int elementCount,
            long retainedSourceBytes,
            Consumer<IntConsumer> payloadLengths,
            Consumer<ReplySink> emitter
    ) implements RedisReply {
        public ByteSequence {
            requireNonNegative(elementCount, "elementCount");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            Objects.requireNonNull(emitter, "emitter");
        }
    }

    record ByteSet(
            int elementCount,
            long retainedSourceBytes,
            Consumer<IntConsumer> payloadLengths,
            Consumer<ReplySink> emitter
    ) implements RedisReply {
        public ByteSet {
            requireNonNegative(elementCount, "elementCount");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            Objects.requireNonNull(emitter, "emitter");
        }
    }

    record ByteMap(
            int pairCount,
            long retainedSourceBytes,
            Consumer<IntConsumer> payloadLengths,
            Consumer<ReplySink> emitter
    ) implements RedisReply {
        public ByteMap {
            requireNonNegative(pairCount, "pairCount");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            Objects.requireNonNull(emitter, "emitter");
        }
    }

    private static ReplyShape aggregateShape(Aggregate aggregate) {
        List<ReplyShape> shapes = new ArrayList<>(aggregate.elements.size());
        for (RedisReply element : aggregate.elements) {
            shapes.add(element.shape());
        }
        return switch (aggregate.kind) {
            case ARRAY -> ReplyShapes.array(shapes);
            case MAP -> ReplyShapes.map(shapes);
            case SET -> ReplyShapes.set(shapes);
            case PUSH -> ReplyShapes.push(shapes);
            case ATTRIBUTE -> ReplyShapes.attribute(shapes);
        };
    }

    private static List<RedisReply> copyAggregateElements(
            ReplyShape.AggregateKind kind,
            List<RedisReply> elements
    ) {
        List<RedisReply> copied = List.copyOf(Objects.requireNonNull(elements, "elements"));
        if ((kind == ReplyShape.AggregateKind.MAP || kind == ReplyShape.AggregateKind.ATTRIBUTE)
                && (copied.size() & 1) != 0) {
            throw new IllegalArgumentException(kind + " requires field/value pairs");
        }
        for (RedisReply element : copied) {
            if (element instanceof ControlError) {
                throw new IllegalArgumentException("control error must be a top-level reply");
            }
        }
        return copied;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
