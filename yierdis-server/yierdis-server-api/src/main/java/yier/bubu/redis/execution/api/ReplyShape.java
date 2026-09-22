package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * 已完成命令语义所需的回复形状，不包含任何具体协议的编码大小。
 */
public sealed interface ReplyShape permits
        ReplyShape.SimpleString,
        ReplyShape.Error,
        ReplyShape.IntegerValue,
        ReplyShape.BulkString,
        ReplyShape.NullValue,
        ReplyShape.NullArray,
        ReplyShape.Aggregate,
        ReplyShape.ByteAggregate,
        ReplyShape.Maximum {

    default long retainedSourceBytes() {
        return 0L;
    }

    enum AggregateKind {
        ARRAY,
        MAP
    }

    record SimpleString(int payloadLength) implements ReplyShape {
        public SimpleString {
            requireNonNegative(payloadLength, "payloadLength");
        }
    }

    record Error(int payloadLength) implements ReplyShape {
        public Error {
            requireNonNegative(payloadLength, "payloadLength");
        }
    }

    record IntegerValue(long value) implements ReplyShape {
    }

    record BulkString(int payloadLength, long retainedSourceBytes) implements ReplyShape {
        public BulkString {
            requireNonNegative(payloadLength, "payloadLength");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
        }
    }

    record NullValue() implements ReplyShape {
    }

    record NullArray() implements ReplyShape {
    }

    record Aggregate(
            AggregateKind kind,
            List<ReplyShape> elements,
            long retainedSourceBytes
    ) implements ReplyShape {
        public Aggregate {
            Objects.requireNonNull(kind, "kind");
            elements = List.copyOf(Objects.requireNonNull(elements, "elements"));
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
            for (ReplyShape element : elements) {
                if (element instanceof Maximum) {
                    throw new IllegalArgumentException("maximum reservation must be top-level");
                }
            }
        }
    }

    enum ByteAggregateKind {
        SEQUENCE,
        SET,
        MAP
    }

    /**
     * 流式字节聚合：SEQUENCE/SET 的 count 是元素数，MAP 的 count 是键值对数。
     */
    record ByteAggregate(
            ByteAggregateKind kind,
            int count,
            Consumer<IntConsumer> payloadLengths,
            long retainedSourceBytes
    ) implements ReplyShape {
        public ByteAggregate {
            Objects.requireNonNull(kind, "kind");
            requireNonNegative(count, "count");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
        }
    }

    record Maximum() implements ReplyShape {
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
