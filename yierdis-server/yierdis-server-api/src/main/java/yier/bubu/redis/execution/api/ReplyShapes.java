package yier.bubu.redis.execution.api;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * 构造协议无关回复形状的工厂。
 */
public final class ReplyShapes {
    private static final int MAX_NORMALIZED_ERROR_BYTES = 512;

    private ReplyShapes() {
    }

    public static ReplyShape simpleString(String value) {
        return new ReplyShape.SimpleString(asciiLength(value));
    }

    public static ReplyShape error(String value) {
        return new ReplyShape.Error(asciiLength(normalizeError(value)));
    }

    public static ReplyShape errorUpperBound() {
        return new ReplyShape.Error(MAX_NORMALIZED_ERROR_BYTES);
    }

    public static ReplyShape integer(long value) {
        return new ReplyShape.IntegerValue(value);
    }

    public static ReplyShape integerUpperBound() {
        return new ReplyShape.IntegerValue(Long.MIN_VALUE);
    }

    public static ReplyShape bulkString(int payloadLength, long retainedSourceBytes) {
        return new ReplyShape.BulkString(payloadLength, retainedSourceBytes);
    }

    public static ReplyShape nullValue() {
        return new ReplyShape.NullValue();
    }

    public static ReplyShape nullArray() {
        return new ReplyShape.NullArray();
    }

    public static ReplyShape array(List<? extends ReplyShape> elements) {
        return aggregate(ReplyShape.AggregateKind.ARRAY, elements);
    }

    public static ReplyShape map(List<? extends ReplyShape> fieldValues) {
        return aggregate(ReplyShape.AggregateKind.MAP, fieldValues);
    }

    public static ReplyShape sequence(
            int count,
            long retainedSourceBytes,
            Consumer<IntConsumer> lengths
    ) {
        return new ReplyShape.ByteSequence(count, lengths, retainedSourceBytes);
    }

    public static ReplyShape byteSet(
            int count,
            long retainedSourceBytes,
            Consumer<IntConsumer> lengths
    ) {
        return new ReplyShape.ByteSet(count, lengths, retainedSourceBytes);
    }

    public static ReplyShape byteMap(
            int pairCount,
            long retainedSourceBytes,
            Consumer<IntConsumer> lengths
    ) {
        return new ReplyShape.ByteMap(pairCount, lengths, retainedSourceBytes);
    }

    public static ReplyShape maximum() {
        return new ReplyShape.Maximum();
    }

    private static ReplyShape aggregate(
            ReplyShape.AggregateKind kind,
            List<? extends ReplyShape> elements
    ) {
        Objects.requireNonNull(elements, "elements");
        List<ReplyShape> copied = List.copyOf(elements);
        if (kind == ReplyShape.AggregateKind.MAP && (copied.size() & 1) != 0) {
            throw new IllegalArgumentException(kind + " requires field/value pairs");
        }

        long retained = 0L;
        for (ReplyShape element : copied) {
            retained = saturatedAdd(retained, element.retainedSourceBytes());
        }
        return new ReplyShape.Aggregate(kind, copied, retained);
    }

    // 子形状可能分别持有独立来源；聚合预留必须保守，溢出时不能回绕成较小额度。
    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static int asciiLength(String value) {
        return (value == null ? "" : value).getBytes(StandardCharsets.US_ASCII).length;
    }

    private static int utf8Length(String value) {
        return (value == null ? "" : value).getBytes(StandardCharsets.UTF_8).length;
    }

    public static String normalizeError(String message) {
        String value = sanitizeSimple(message == null ? "ERR error" : message);
        if (!hasRedisErrorPrefix(value)) {
            value = "ERR " + value;
        }
        return truncateUtf8(value, MAX_NORMALIZED_ERROR_BYTES);
    }

    public static String sanitizeSimple(String value) {
        return value == null ? "" : value.replace('\r', ' ').replace('\n', ' ');
    }

    private static boolean hasRedisErrorPrefix(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        int end = 0;
        while (end < value.length()) {
            char ch = value.charAt(end);
            if (Character.isWhitespace(ch)) {
                break;
            }
            if (!(ch == '-' || ch == '_' || Character.isDigit(ch) || Character.isUpperCase(ch))) {
                return false;
            }
            end++;
        }
        return end > 0 && (end == value.length() || Character.isWhitespace(value.charAt(end)));
    }

    private static String truncateUtf8(String value, int maxBytes) {
        if (utf8Length(value) <= maxBytes) {
            return value;
        }
        int end = 0;
        int used = 0;
        while (end < value.length()) {
            int codePoint = value.codePointAt(end);
            int codePointBytes = utf8Length(codePoint);
            if (used + codePointBytes > maxBytes) {
                break;
            }
            used += codePointBytes;
            end += Character.charCount(codePoint);
        }
        return value.substring(0, end);
    }

    private static int utf8Length(int codePoint) {
        if (codePoint <= 0x7F) {
            return 1;
        }
        if (codePoint <= 0x7FF) {
            return 2;
        }
        if (codePoint <= 0xFFFF) {
            return 3;
        }
        return 4;
    }
}
