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
        ReplyShape.BooleanValue,
        ReplyShape.DoubleValue,
        ReplyShape.BigNumber,
        ReplyShape.VerbatimString,
        ReplyShape.BlobError,
        ReplyShape.BulkString,
        ReplyShape.NullValue,
        ReplyShape.NullArray,
        ReplyShape.Aggregate,
        ReplyShape.ByteSequence,
        ReplyShape.ByteSet,
        ReplyShape.ByteMap,
        ReplyShape.Maximum {

    default long retainedSourceBytes() {
        return 0L;
    }

    enum AggregateKind {
        ARRAY,
        MAP,
        SET,
        PUSH,
        ATTRIBUTE
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

    record BooleanValue(boolean value) implements ReplyShape {
    }

    record DoubleValue(double value) implements ReplyShape {
    }

    record BigNumber(int asciiLength) implements ReplyShape {
        public BigNumber {
            requireNonNegative(asciiLength, "asciiLength");
        }
    }

    record VerbatimString(int formatLength, int payloadLength) implements ReplyShape {
        public VerbatimString {
            requireNonNegative(formatLength, "formatLength");
            requireNonNegative(payloadLength, "payloadLength");
        }
    }

    record BlobError(int payloadLength) implements ReplyShape {
        public BlobError {
            requireNonNegative(payloadLength, "payloadLength");
        }
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

    record ByteSequence(
            int elementCount,
            Consumer<IntConsumer> payloadLengths,
            long retainedSourceBytes
    ) implements ReplyShape {
        public ByteSequence {
            requireNonNegative(elementCount, "elementCount");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
        }
    }

    record ByteSet(
            int elementCount,
            Consumer<IntConsumer> payloadLengths,
            long retainedSourceBytes
    ) implements ReplyShape {
        public ByteSet {
            requireNonNegative(elementCount, "elementCount");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
        }
    }

    record ByteMap(
            int pairCount,
            Consumer<IntConsumer> payloadLengths,
            long retainedSourceBytes
    ) implements ReplyShape {
        public ByteMap {
            requireNonNegative(pairCount, "pairCount");
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
