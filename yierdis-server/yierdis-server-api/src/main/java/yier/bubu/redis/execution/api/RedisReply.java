package yier.bubu.redis.execution.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

public sealed interface RedisReply permits
        RedisReply.SimpleString, RedisReply.Error, RedisReply.ControlError,
        RedisReply.IntegerValue, RedisReply.BulkString, RedisReply.NullValue, RedisReply.NullArray,
        RedisReply.Aggregate, RedisReply.ByteAggregate {

    default ReplyShape shape() {
        return switch (this) {
            case SimpleString value -> ReplyShapes.simpleString(value.value);
            case Error value -> ReplyShapes.error(value.message);
            case ControlError ignored -> ReplyShapes.maximum();
            case IntegerValue value -> ReplyShapes.integer(value.value);
            case BulkString value -> ReplyShapes.bulkString(value.payloadLength, value.retainedSourceBytes);
            case NullValue ignored -> ReplyShapes.nullValue();
            case NullArray ignored -> ReplyShapes.nullArray();
            case Aggregate value -> aggregateShape(value);
            case ByteAggregate value -> ReplyShapes.byteAggregate(
                    value.kind, value.count, value.retainedSourceBytes, value.payloadLengths);
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

    /**
     * 流式字节聚合：SEQUENCE/SET 的 count 是元素数，MAP 的 count 是键值对数。
     */
    record ByteAggregate(
            ReplyShape.ByteAggregateKind kind,
            int count,
            long retainedSourceBytes,
            Consumer<IntConsumer> payloadLengths,
            Consumer<ReplySink> emitter
    ) implements RedisReply {
        public ByteAggregate {
            Objects.requireNonNull(kind, "kind");
            requireNonNegative(count, "count");
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
        };
    }

    private static List<RedisReply> copyAggregateElements(
            ReplyShape.AggregateKind kind,
            List<RedisReply> elements
    ) {
        List<RedisReply> copied = List.copyOf(Objects.requireNonNull(elements, "elements"));
        if (kind == ReplyShape.AggregateKind.MAP && (copied.size() & 1) != 0) {
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
