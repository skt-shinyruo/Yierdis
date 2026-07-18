package yier.bubu.redis.app.bench.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public final class IncrementalRespReplyDecoder {
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final NeedMoreData NEED_MORE_DATA = new NeedMoreData();

    private final int maxBulkBytes;
    private final int maxLineBytes;
    private final int maxArrayLength;
    private final int maxDepth;

    public IncrementalRespReplyDecoder(
            int maxBulkBytes,
            int maxLineBytes,
            int maxArrayLength,
            int maxDepth
    ) {
        this.maxBulkBytes = requireNonNegative(maxBulkBytes, "maxBulkBytes");
        this.maxLineBytes = requireNonNegative(maxLineBytes, "maxLineBytes");
        this.maxArrayLength = requireNonNegative(maxArrayLength, "maxArrayLength");
        this.maxDepth = requireNonNegative(maxDepth, "maxDepth");
    }

    public BenchmarkRespReply tryDecode(ByteBuffer input) throws IOException {
        Objects.requireNonNull(input, "input");
        int start = input.position();
        try {
            return decode(input, 0, true);
        } catch (NeedMoreData incomplete) {
            input.position(start);
            return null;
        }
    }

    private BenchmarkRespReply decode(ByteBuffer input, int depth, boolean materialize)
            throws IOException, NeedMoreData {
        if (depth > maxDepth) {
            throw malformed("reply nesting depth exceeds " + maxDepth);
        }
        if (!input.hasRemaining()) {
            throw NEED_MORE_DATA;
        }

        byte marker = input.get();
        return switch (marker) {
            case '+' -> decodeText(input, materialize, false);
            case '-' -> decodeText(input, materialize, true);
            case ':' -> decodeInteger(input, materialize);
            case '$' -> decodeBulk(input, materialize);
            case '*' -> decodeArray(input, depth, materialize);
            default -> throw malformed("unsupported reply marker " + printable(marker));
        };
    }

    private BenchmarkRespReply decodeText(ByteBuffer input, boolean materialize, boolean error)
            throws IOException, NeedMoreData {
        int lineStart = input.position();
        int lineEnd = readLineEnd(input);
        if (!materialize) {
            return null;
        }

        String text = decodeText(input, lineStart, lineEnd);
        return error ? BenchmarkRespReply.error(text) : BenchmarkRespReply.simpleString(text);
    }

    private BenchmarkRespReply decodeInteger(ByteBuffer input, boolean materialize)
            throws IOException, NeedMoreData {
        long value = readLong(input);
        return materialize ? BenchmarkRespReply.integer(value) : null;
    }

    private BenchmarkRespReply decodeBulk(ByteBuffer input, boolean materialize)
            throws IOException, NeedMoreData {
        long declaredLength = readLong(input);
        if (declaredLength == -1) {
            return materialize ? BenchmarkRespReply.nullBulk() : null;
        }
        if (declaredLength < 0) {
            throw malformed("bulk length must be non-negative or -1");
        }
        if (declaredLength > maxBulkBytes) {
            throw malformed("bulk length exceeds " + maxBulkBytes);
        }

        int length = (int) declaredLength;
        int payloadStart = input.position();
        if ((long) input.limit() - payloadStart < length) {
            throw NEED_MORE_DATA;
        }

        int payloadEnd = payloadStart + length;
        if (payloadEnd == input.limit()) {
            throw NEED_MORE_DATA;
        }
        if (input.get(payloadEnd) != CR) {
            throw malformed("bulk payload is not followed by CRLF");
        }
        if (payloadEnd + 1 == input.limit()) {
            throw NEED_MORE_DATA;
        }
        if (input.get(payloadEnd + 1) != LF) {
            throw malformed("bulk payload is not followed by CRLF");
        }

        input.position(payloadEnd + 2);
        return materialize ? BenchmarkRespReply.bulkString(length) : null;
    }

    private BenchmarkRespReply decodeArray(ByteBuffer input, int depth, boolean materialize)
            throws IOException, NeedMoreData {
        long declaredLength = readLong(input);
        if (declaredLength == -1) {
            return materialize ? BenchmarkRespReply.nullArray() : null;
        }
        if (declaredLength < 0) {
            throw malformed("array length must be non-negative or -1");
        }
        if (declaredLength > maxArrayLength) {
            throw malformed("array length exceeds " + maxArrayLength);
        }

        int length = (int) declaredLength;
        for (int index = 0; index < length; index++) {
            decode(input, depth + 1, false);
        }
        return materialize ? BenchmarkRespReply.array(length) : null;
    }

    private long readLong(ByteBuffer input) throws IOException, NeedMoreData {
        int lineStart = input.position();
        int lineEnd = readLineEnd(input);
        if (lineStart == lineEnd) {
            throw malformed("numeric field is empty");
        }

        int index = lineStart;
        boolean negative = input.get(index) == '-';
        if (negative && ++index == lineEnd) {
            throw malformed("numeric field has no digits");
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multiplyLimit = limit / 10;
        long result = 0;
        while (index < lineEnd) {
            byte current = input.get(index++);
            if (current < '0' || current > '9') {
                throw malformed("numeric field contains a non-digit");
            }
            int digit = current - '0';
            if (result < multiplyLimit) {
                throw malformed("numeric field overflows a signed long");
            }
            result *= 10;
            if (result < limit + digit) {
                throw malformed("numeric field overflows a signed long");
            }
            result -= digit;
        }
        return negative ? result : -result;
    }

    private int readLineEnd(ByteBuffer input) throws IOException, NeedMoreData {
        int start = input.position();
        int limit = input.limit();
        for (int index = start; index < limit; index++) {
            byte current = input.get(index);
            if (current == CR) {
                if (index + 1 == limit) {
                    throw NEED_MORE_DATA;
                }
                if (input.get(index + 1) != LF) {
                    throw malformed("CR is not followed by LF");
                }
                input.position(index + 2);
                return index;
            }
            if (current == LF) {
                throw malformed("LF is not preceded by CR");
            }
            if (index - start >= maxLineBytes) {
                throw malformed("line exceeds " + maxLineBytes + " bytes");
            }
        }
        throw NEED_MORE_DATA;
    }

    private static String decodeText(ByteBuffer input, int start, int end) {
        int length = end - start;
        if (length == 0) {
            return "";
        }

        char[] text = new char[length];
        for (int index = 0; index < length; index++) {
            text[index] = (char) (input.get(start + index) & 0xff);
        }
        return new String(text);
    }

    private static int requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static IOException malformed(String detail) {
        return new IOException("Malformed RESP2 reply: " + detail);
    }

    private static String printable(byte value) {
        int unsigned = value & 0xff;
        if (unsigned >= 0x20 && unsigned <= 0x7e) {
            return "'" + (char) unsigned + "'";
        }
        return "0x" + Integer.toHexString(unsigned);
    }

    private static final class NeedMoreData extends Exception {
        private NeedMoreData() {
            super(null, null, false, false);
        }
    }
}
