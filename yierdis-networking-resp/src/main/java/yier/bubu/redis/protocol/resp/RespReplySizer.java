package yier.bubu.redis.protocol.resp;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import java.util.function.BiFunction;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyShape;

/**
 * RESP 协议对语义回复形状的唯一容量计算实现。
 *
 * <p>协议版本由调用方在 prepare/预留时刻确定并传入，计算结果把它原样捕获进 {@link ReplyPlan}。</p>
 */
public final class RespReplySizer implements BiFunction<Integer, ReplyShape, ReplyPlan> {
    @Override
    public ReplyPlan apply(Integer protocolVersion, ReplyShape shape) {
        Objects.requireNonNull(protocolVersion, "protocolVersion");
        Objects.requireNonNull(shape, "shape");
        RespProtocolVersion version = RespProtocolVersion.fromWireValue(protocolVersion);
        if (requiresMaximumReservation(shape)) {
            return ReplyPlan.maximum(protocolVersion);
        }
        return ReplyPlan.exact(encodedBytes(shape, version), shape.retainedSourceBytes(), protocolVersion);
    }

    private static long encodedBytes(ReplyShape shape, RespProtocolVersion version) {
        return switch (shape) {
            case ReplyShape.SimpleString value -> lineBytes(value.payloadLength());
            case ReplyShape.Error value -> lineBytes(value.payloadLength());
            case ReplyShape.IntegerValue value -> lineBytes(decimalDigits(value.value()));
            case ReplyShape.BulkString value -> bulkBytes(value.payloadLength());
            case ReplyShape.NullValue ignored -> version.nullValueEncoding().length;
            case ReplyShape.NullArray ignored -> version.nullArrayEncoding().length;
            case ReplyShape.Aggregate value -> aggregateBytes(value, version);
            case ReplyShape.ByteAggregate value -> byteAggregateBytes(value, version);
            case ReplyShape.Maximum ignored -> throw new AssertionError("maximum was handled before sizing");
        };
    }

    private static boolean requiresMaximumReservation(ReplyShape shape) {
        return switch (shape) {
            case ReplyShape.Maximum ignored -> true;
            default -> false;
        };
    }

    private static long aggregateBytes(ReplyShape.Aggregate aggregate, RespProtocolVersion version) {
        List<ReplyShape> elements = aggregate.elements();
        long encoded = switch (aggregate.kind()) {
            case ARRAY -> aggregateHeaderBytes('*', elements.size());
            case MAP -> mapHeaderBytes(elements.size(), version);
        };
        for (ReplyShape element : elements) {
            encoded = addSaturating(encoded, encodedBytes(element, version));
        }
        return encoded;
    }

    private static long byteAggregateBytes(ReplyShape.ByteAggregate aggregate, RespProtocolVersion version) {
        ReplyShape.ByteAggregateKind kind = aggregate.kind();
        long expectedValues = kind == ReplyShape.ByteAggregateKind.MAP
                ? Math.multiplyExact((long) aggregate.count(), 2L)
                : aggregate.count();
        PayloadAccumulator payloads = new PayloadAccumulator(expectedValues, version);
        aggregate.payloadLengths().accept(payloads::accept);
        payloads.verifyComplete(kind.name().toLowerCase(Locale.ROOT));
        long header = switch (kind) {
            case SEQUENCE -> aggregateHeaderBytes('*', aggregate.count());
            case SET -> aggregateHeaderBytes(version.setPrefix(), aggregate.count());
            case MAP -> aggregateHeaderBytes(version.mapPrefix(), version.mapHeaderCount(aggregate.count()));
        };
        return addSaturating(header, payloads.encodedBytes());
    }

    private static long mapHeaderBytes(int elementCount, RespProtocolVersion version) {
        if ((elementCount & 1) != 0) {
            throw new IllegalArgumentException("map-like aggregate requires field/value pairs");
        }
        return aggregateHeaderBytes(version.mapPrefix(), version.mapHeaderCount(elementCount / 2L));
    }

    private static long lineBytes(long payloadLength) {
        return addSaturating(payloadLength, 3L);
    }

    private static long framedBytes(long payloadLength) {
        return addSaturating(addSaturating(decimalDigits(payloadLength), 3L), addSaturating(payloadLength, 2L));
    }

    private static long bulkBytes(int payloadLength) {
        return framedBytes(payloadLength);
    }

    private static long aggregateHeaderBytes(char prefix, long count) {
        if (count < 0L) {
            throw new IllegalArgumentException("aggregate count must be non-negative");
        }
        return addSaturating(decimalDigits(count), 3L);
    }

    private static int decimalDigits(long value) {
        return Long.toString(value).length();
    }

    private static final class PayloadAccumulator {
        private final long expectedCount;
        private final RespProtocolVersion version;
        private long actualCount;
        private long encodedBytes;

        private PayloadAccumulator(long expectedCount, RespProtocolVersion version) {
            this.expectedCount = expectedCount;
            this.version = version;
        }

        private void accept(int payloadLength) {
            // 回调来自可重复访问的语义来源；必须在预留前验证完整性，不能在写出部分回复后才发现不一致。
            if (payloadLength < -1) {
                throw new IllegalArgumentException("semantic payload length must be >= -1");
            }
            actualCount++;
            if (actualCount > expectedCount) {
                throw new IllegalArgumentException("semantic payload callback emitted too many values");
            }
            encodedBytes = addSaturating(encodedBytes,
                    payloadLength == -1 ? version.nullValueEncoding().length : bulkBytes(payloadLength));
        }

        private void verifyComplete(String kind) {
            if (actualCount != expectedCount) {
                throw new IllegalArgumentException(
                        kind + " semantic payload callback emitted " + actualCount
                                + " values, expected " + expectedCount);
            }
        }

        private long encodedBytes() {
            return encodedBytes;
        }
    }
}
