package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.Objects;

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

    long retainedSourceBytes();

    @FunctionalInterface
    interface PayloadLengths {
        void visit(java.util.function.IntConsumer consumer);
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

        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record Error(int payloadLength) implements ReplyShape {
        public Error {
            requireNonNegative(payloadLength, "payloadLength");
        }

        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record IntegerValue(long value) implements ReplyShape {
        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record BooleanValue(boolean value) implements ReplyShape {
        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record DoubleValue(double value) implements ReplyShape {
        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record BigNumber(int asciiLength) implements ReplyShape {
        public BigNumber {
            requireNonNegative(asciiLength, "asciiLength");
        }

        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record VerbatimString(int formatLength, int payloadLength) implements ReplyShape {
        public VerbatimString {
            requireNonNegative(formatLength, "formatLength");
            requireNonNegative(payloadLength, "payloadLength");
        }

        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record BlobError(int payloadLength) implements ReplyShape {
        public BlobError {
            requireNonNegative(payloadLength, "payloadLength");
        }

        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record BulkString(int payloadLength, long retainedSourceBytes) implements ReplyShape {
        public BulkString {
            requireNonNegative(payloadLength, "payloadLength");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
        }
    }

    record NullValue() implements ReplyShape {
        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    record NullArray() implements ReplyShape {
        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
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
            PayloadLengths payloadLengths,
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
            PayloadLengths payloadLengths,
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
            PayloadLengths payloadLengths,
            long retainedSourceBytes
    ) implements ReplyShape {
        public ByteMap {
            requireNonNegative(pairCount, "pairCount");
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
        }
    }

    record Maximum() implements ReplyShape {
        @Override
        public long retainedSourceBytes() {
            return 0L;
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
