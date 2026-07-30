package yier.bubu.redis.execution.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public sealed interface RedisReply permits
        RedisReply.SimpleString, RedisReply.Error, RedisReply.ControlError,
        RedisReply.IntegerValue, RedisReply.BooleanValue, RedisReply.DoubleValue,
        RedisReply.BigNumber, RedisReply.VerbatimString, RedisReply.BlobError,
        RedisReply.BulkString, RedisReply.NullValue, RedisReply.NullArray,
        RedisReply.Aggregate, RedisReply.ByteSequence, RedisReply.ByteMap {

    ReplyShape shape();

    @FunctionalInterface
    interface PayloadEmitter {
        void emit(ReplySink sink);
    }

    record SimpleString(String value) implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.simpleString(value);
        }
    }

    record Error(String message) implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.error(message);
        }
    }

    record ControlError(String message) implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.maximum();
        }
    }

    record IntegerValue(long value) implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.integer(value);
        }
    }

    record BooleanValue(boolean value) implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.booleanValue(value);
        }
    }

    record DoubleValue(double value) implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.doubleValue(value);
        }
    }

    record BigNumber(String ascii) implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.bigNumber(ascii);
        }
    }

    record VerbatimString(String format, byte[] data) implements RedisReply {
        public VerbatimString {
            data = Objects.requireNonNull(data, "data").clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }

        @Override
        public ReplyShape shape() {
            return ReplyShapes.verbatimString(format, data.length);
        }
    }

    record BlobError(String message) implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.blobError(message);
        }
    }

    record BulkString(
            int payloadLength,
            long retainedSourceBytes,
            PayloadEmitter emitter
    ) implements RedisReply {
        public BulkString {
            requireNonNegative(payloadLength, "payloadLength");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
            Objects.requireNonNull(emitter, "emitter");
        }

        @Override
        public ReplyShape shape() {
            return ReplyShapes.bulkString(payloadLength, retainedSourceBytes);
        }
    }

    record NullValue() implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.nullValue();
        }
    }

    record NullArray() implements RedisReply {
        @Override
        public ReplyShape shape() {
            return ReplyShapes.nullArray();
        }
    }

    record Aggregate(ReplyShape.AggregateKind kind, List<RedisReply> elements) implements RedisReply {
        public Aggregate {
            kind = Objects.requireNonNull(kind, "kind");
            elements = copyAggregateElements(kind, elements);
        }

        @Override
        public ReplyShape shape() {
            List<ReplyShape> shapes = new ArrayList<>(elements.size());
            for (RedisReply element : elements) {
                shapes.add(element.shape());
            }
            return switch (kind) {
                case ARRAY -> ReplyShapes.array(shapes);
                case MAP -> ReplyShapes.map(shapes);
                case SET -> ReplyShapes.set(shapes);
                case PUSH -> ReplyShapes.push(shapes);
                case ATTRIBUTE -> ReplyShapes.attribute(shapes);
            };
        }
    }

    record ByteSequence(
            int elementCount,
            long retainedSourceBytes,
            ReplyShape.PayloadLengths payloadLengths,
            PayloadEmitter emitter
    ) implements RedisReply {
        public ByteSequence {
            requireNonNegative(elementCount, "elementCount");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            Objects.requireNonNull(emitter, "emitter");
        }

        @Override
        public ReplyShape shape() {
            return ReplyShapes.sequence(elementCount, retainedSourceBytes, payloadLengths);
        }
    }

    record ByteMap(
            int pairCount,
            long retainedSourceBytes,
            ReplyShape.PayloadLengths payloadLengths,
            PayloadEmitter emitter
    ) implements RedisReply {
        public ByteMap {
            requireNonNegative(pairCount, "pairCount");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            Objects.requireNonNull(emitter, "emitter");
        }

        @Override
        public ReplyShape shape() {
            return ReplyShapes.byteMap(pairCount, retainedSourceBytes, payloadLengths);
        }
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
