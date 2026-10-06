package yier.bubu.redis.execution.api;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

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

    public static ReplyShape byteAggregate(
            ReplyShape.ByteAggregateKind kind,
            int count,
            long retainedSourceBytes,
            Consumer<IntConsumer> lengths
    ) {
        return new ReplyShape.ByteAggregate(kind, count, lengths, retainedSourceBytes);
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

        // 子形状可能分别持有独立来源；聚合预留必须保守，溢出时不能回绕成较小额度。
        long retained = 0L;
        for (ReplyShape element : copied) {
            retained = addSaturating(retained, element.retainedSourceBytes());
        }
        return new ReplyShape.Aggregate(kind, copied, retained);
    }

    private static int asciiLength(String value) {
        return (value == null ? "" : value).getBytes(StandardCharsets.US_ASCII).length;
    }

    public static String normalizeError(String message) {
        String value = sanitizeSimple(message);
        if (value.isBlank()) {
            value = "ERR error";
        } else if (!hasRedisErrorPrefix(value)) {
            value = "ERR " + value;
        }
        value = truncateUtf8(value, MAX_NORMALIZED_ERROR_BYTES);
        // 截断或丢弃孤立 surrogate 后，不能留下只有前缀和空白的 "ERR "。
        if (value.isBlank() || (value.startsWith("ERR") && value.length() > 3 && value.substring(3).isBlank())) {
            return "ERR error";
        }
        return value;
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

    /**
     * 按 Unicode code point 截断。有效代理对是一个 code point，不会拆成孤立代理项。
     * 孤立 surrogate 不是合法标量，直接丢弃，避免结果无法编码成规范 UTF-8。
     */
    private static String truncateUtf8(String value, int maxBytes) {
        StringBuilder kept = new StringBuilder(value.length());
        int used = 0;
        int index = 0;
        boolean changed = false;
        while (index < value.length()) {
            int codePoint = value.codePointAt(index);
            int charCount = Character.charCount(codePoint);
            if (codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE) {
                changed = true;
                index += charCount;
                continue;
            }
            int codePointBytes = utf8Length(codePoint);
            if (used + codePointBytes > maxBytes) {
                changed = true;
                break;
            }
            kept.appendCodePoint(codePoint);
            used += codePointBytes;
            index += charCount;
        }
        if (!changed && index == value.length()) {
            return value;
        }
        return kept.toString();
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
