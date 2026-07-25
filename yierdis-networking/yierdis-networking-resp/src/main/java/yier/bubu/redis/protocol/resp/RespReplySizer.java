package yier.bubu.redis.protocol.resp;

import java.util.List;
import java.util.Objects;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplySizer;

/**
 * RESP 协议对语义回复形状的唯一容量计算实现。
 */
public final class RespReplySizer implements ReplySizer {
    @Override
    public ReplyPlan plan(CommandSession session, ReplyShape shape) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(shape, "shape");
        if (requiresMaximumReservation(shape)) {
            return ReplyPlan.maximum();
        }
        RespProtocolVersion version = RespProtocolVersion.fromWireValue(session.respVersion());
        return ReplyPlan.exact(encodedBytes(shape, version), shape.retainedSourceBytes());
    }

    private static long encodedBytes(ReplyShape shape, RespProtocolVersion version) {
        return switch (shape) {
            case ReplyShape.SimpleString value -> lineBytes(value.payloadLength());
            case ReplyShape.Error value -> lineBytes(value.payloadLength());
            case ReplyShape.IntegerValue value -> lineBytes(decimalDigits(value.value()));
            case ReplyShape.BooleanValue value -> version == RespProtocolVersion.RESP3
                    ? 4L
                    : lineBytes(1);
            case ReplyShape.DoubleValue value -> doubleBytes(value.value(), version);
            case ReplyShape.BigNumber value -> version == RespProtocolVersion.RESP3
                    ? lineBytes(value.asciiLength())
                    : bulkBytes(value.asciiLength());
            case ReplyShape.VerbatimString value -> version == RespProtocolVersion.RESP3
                    ? framedBytes(saturatedAdd(saturatedAdd(value.formatLength(), 1L), value.payloadLength()))
                    : bulkBytes(value.payloadLength());
            case ReplyShape.BlobError value -> version == RespProtocolVersion.RESP3
                    ? framedBytes(value.payloadLength())
                    : lineBytes(value.payloadLength());
            case ReplyShape.BulkString value -> bulkBytes(value.payloadLength());
            case ReplyShape.NullValue ignored -> nullValueBytes(version);
            case ReplyShape.NullArray ignored -> nullArrayBytes(version);
            case ReplyShape.Aggregate value -> aggregateBytes(value, version);
            case ReplyShape.Alternatives value -> alternativesBytes(value, version);
            case ReplyShape.ByteSequence value -> byteSequenceBytes(value, version);
            case ReplyShape.ByteMap value -> byteMapBytes(value, version);
            case ReplyShape.Maximum ignored -> throw new AssertionError("maximum was handled before sizing");
        };
    }

    private static boolean requiresMaximumReservation(ReplyShape shape) {
        return switch (shape) {
            case ReplyShape.Maximum ignored -> true;
            case ReplyShape.Alternatives value -> value.alternatives().stream()
                    .anyMatch(RespReplySizer::requiresMaximumReservation);
            default -> false;
        };
    }

    private static long alternativesBytes(ReplyShape.Alternatives alternatives, RespProtocolVersion version) {
        long maximum = 0L;
        for (ReplyShape alternative : alternatives.alternatives()) {
            maximum = Math.max(maximum, encodedBytes(alternative, version));
        }
        return maximum;
    }

    private static long doubleBytes(double value, RespProtocolVersion version) {
        String text;
        if (Double.isNaN(value)) {
            text = "nan";
        } else if (value == Double.POSITIVE_INFINITY) {
            text = "inf";
        } else if (value == Double.NEGATIVE_INFINITY) {
            text = "-inf";
        } else {
            text = Double.toString(value);
        }
        return version == RespProtocolVersion.RESP3
                ? lineBytes(text.length())
                : bulkBytes(text.length());
    }

    private static long aggregateBytes(ReplyShape.Aggregate aggregate, RespProtocolVersion version) {
        List<ReplyShape> elements = aggregate.elements();
        long encoded = switch (aggregate.kind()) {
            case ARRAY -> aggregateHeaderBytes('*', elements.size());
            case MAP -> mapHeaderBytes(elements.size(), version, '%');
            case SET -> version == RespProtocolVersion.RESP3
                    ? aggregateHeaderBytes('~', elements.size())
                    : aggregateHeaderBytes('*', elements.size());
            case PUSH -> version == RespProtocolVersion.RESP3
                    ? aggregateHeaderBytes('>', elements.size())
                    : aggregateHeaderBytes('*', elements.size());
            case ATTRIBUTE -> mapHeaderBytes(elements.size(), version, '|');
        };
        for (ReplyShape element : elements) {
            encoded = saturatedAdd(encoded, encodedBytes(element, version));
        }
        return encoded;
    }

    private static long byteSequenceBytes(ReplyShape.ByteSequence sequence, RespProtocolVersion version) {
        PayloadAccumulator payloads = new PayloadAccumulator(sequence.elementCount(), version);
        sequence.payloadLengths().visit(payloads::accept);
        payloads.verifyComplete("sequence");
        return saturatedAdd(aggregateHeaderBytes('*', sequence.elementCount()), payloads.encodedBytes());
    }

    private static long byteMapBytes(ReplyShape.ByteMap map, RespProtocolVersion version) {
        long expectedValues = Math.multiplyExact((long) map.pairCount(), 2L);
        PayloadAccumulator payloads = new PayloadAccumulator(expectedValues, version);
        map.payloadLengths().visit(payloads::accept);
        payloads.verifyComplete("map");
        long header = version == RespProtocolVersion.RESP3
                ? aggregateHeaderBytes('%', map.pairCount())
                : aggregateHeaderBytes('*', expectedValues);
        return saturatedAdd(header, payloads.encodedBytes());
    }

    private static long mapHeaderBytes(
            int elementCount,
            RespProtocolVersion version,
            char resp3Prefix
    ) {
        if ((elementCount & 1) != 0) {
            throw new IllegalArgumentException("map-like aggregate requires field/value pairs");
        }
        return version == RespProtocolVersion.RESP3
                ? aggregateHeaderBytes(resp3Prefix, elementCount / 2L)
                : aggregateHeaderBytes('*', elementCount);
    }

    private static long nullValueBytes(RespProtocolVersion version) {
        return version == RespProtocolVersion.RESP3 ? 3L : 5L;
    }

    private static long nullArrayBytes(RespProtocolVersion version) {
        return version == RespProtocolVersion.RESP3 ? 3L : 5L;
    }

    private static long lineBytes(long payloadLength) {
        return saturatedAdd(payloadLength, 3L);
    }

    private static long framedBytes(long payloadLength) {
        return saturatedAdd(saturatedAdd(decimalDigits(payloadLength), 3L), saturatedAdd(payloadLength, 2L));
    }

    private static long bulkBytes(int payloadLength) {
        return framedBytes(payloadLength);
    }

    private static long aggregateHeaderBytes(char prefix, long count) {
        if (count < 0L) {
            throw new IllegalArgumentException("aggregate count must be non-negative");
        }
        return saturatedAdd(decimalDigits(count), 3L);
    }

    private static int decimalDigits(long value) {
        return Long.toString(value).length();
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
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
            encodedBytes = saturatedAdd(encodedBytes,
                    payloadLength == -1 ? nullValueBytes(version) : bulkBytes(payloadLength));
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
