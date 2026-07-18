package yier.bubu.redis.app.bench.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class IncrementalRespReplyDecoder {
    static final int MAX_SUPPORTED_DEPTH = 4096;

    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final int INITIAL_LINE_BUFFER_BYTES = 64;

    private final int maxBulkBytes;
    private final int maxLineBytes;
    private final int maxArrayLength;
    private final int maxDepth;
    private final int[] arrayRemaining;

    private byte[] lineBuffer;
    private boolean active;
    private Stage stage = Stage.MARKER;
    private int stackSize;
    private int rootArrayLength = -1;

    private byte currentMarker;
    private boolean currentTopLevel;
    private int lineLength;

    private int bulkLength;
    private int bulkRemaining;

    public IncrementalRespReplyDecoder(
            int maxBulkBytes,
            int maxLineBytes,
            int maxArrayLength,
            int maxDepth
    ) {
        this.maxBulkBytes = requireNonNegative(maxBulkBytes, "maxBulkBytes");
        this.maxLineBytes = requireNonNegative(maxLineBytes, "maxLineBytes");
        this.maxArrayLength = requireNonNegative(maxArrayLength, "maxArrayLength");
        this.maxDepth = requireDepth(maxDepth);
        this.arrayRemaining = new int[maxDepth + 1];
        this.lineBuffer = new byte[Math.min(maxLineBytes, INITIAL_LINE_BUFFER_BYTES)];
    }

    /**
     * 从 input 当前位置继续解码；即使返回 null，也会推进到已消费位置，调用方只需 compact 未消费的尾部。
     */
    public BenchmarkRespReply tryDecode(ByteBuffer input) throws IOException {
        Objects.requireNonNull(input, "input");
        try {
            BenchmarkRespReply reply = advance(input);
            if (reply != null) {
                clearState();
            }
            return reply;
        } catch (IOException failure) {
            clearState();
            throw failure;
        }
    }

    int arrayDepth() {
        return active ? stackSize : 0;
    }

    private BenchmarkRespReply advance(ByteBuffer input) throws IOException {
        while (input.hasRemaining()) {
            BenchmarkRespReply reply = switch (stage) {
                case MARKER -> readMarker(input);
                case LINE -> readLineByte(input);
                case LINE_LF -> finishLine(input);
                case BULK_PAYLOAD -> consumeBulkPayload(input);
                case BULK_CR -> consumeBulkCr(input);
                case BULK_LF -> consumeBulkLf(input);
            };
            if (reply != null) {
                return reply;
            }
        }
        return null;
    }

    private BenchmarkRespReply readMarker(ByteBuffer input) throws IOException {
        currentMarker = input.get();
        if (currentMarker != '+' && currentMarker != '-' && currentMarker != ':'
                && currentMarker != '$' && currentMarker != '*') {
            throw malformed("unsupported reply marker " + printable(currentMarker));
        }

        active = true;
        currentTopLevel = stackSize == 0;
        lineLength = 0;
        stage = Stage.LINE;
        return null;
    }

    private BenchmarkRespReply readLineByte(ByteBuffer input) throws IOException {
        byte current = input.get();
        if (current == CR) {
            stage = Stage.LINE_LF;
            return null;
        }
        if (current == LF) {
            throw malformed("LF is not preceded by CR");
        }
        appendLineByte(current);
        return null;
    }

    private BenchmarkRespReply finishLine(ByteBuffer input) throws IOException {
        if (input.get() != LF) {
            throw malformed("CR is not followed by LF");
        }
        stage = Stage.MARKER;
        return switch (currentMarker) {
            case '+' -> processText(false);
            case '-' -> processText(true);
            case ':' -> processInteger();
            case '$' -> processBulkLength();
            case '*' -> processArrayLength();
            default -> throw new IllegalStateException("validated marker became invalid");
        };
    }

    private BenchmarkRespReply processText(boolean error) {
        if (currentTopLevel) {
            String text = new String(lineBuffer, 0, lineLength, StandardCharsets.UTF_8);
            return error ? BenchmarkRespReply.error(text) : BenchmarkRespReply.simpleString(text);
        }
        return completeDiscardedReply();
    }

    private BenchmarkRespReply processInteger() throws IOException {
        long value = parseLong();
        if (currentTopLevel) {
            return BenchmarkRespReply.integer(value);
        }
        return completeDiscardedReply();
    }

    private BenchmarkRespReply processBulkLength() throws IOException {
        long declaredLength = parseLong();
        if (declaredLength == -1) {
            if (currentTopLevel) {
                return BenchmarkRespReply.nullBulk();
            }
            return completeDiscardedReply();
        }
        if (declaredLength < 0) {
            throw malformed("bulk length must be non-negative or -1");
        }
        if (declaredLength > maxBulkBytes) {
            throw malformed("bulk length exceeds " + maxBulkBytes);
        }

        bulkLength = (int) declaredLength;
        bulkRemaining = bulkLength;
        stage = bulkRemaining == 0 ? Stage.BULK_CR : Stage.BULK_PAYLOAD;
        return null;
    }

    private BenchmarkRespReply processArrayLength() throws IOException {
        long declaredLength = parseLong();
        if (declaredLength == -1) {
            if (currentTopLevel) {
                return BenchmarkRespReply.nullArray();
            }
            return completeDiscardedReply();
        }
        if (declaredLength < 0) {
            throw malformed("array length must be non-negative or -1");
        }
        if (declaredLength > maxArrayLength) {
            throw malformed("array length exceeds " + maxArrayLength);
        }

        int length = (int) declaredLength;
        if (length == 0) {
            if (currentTopLevel) {
                return BenchmarkRespReply.array(0);
            }
            return completeDiscardedReply();
        }
        if (currentTopLevel) {
            rootArrayLength = length;
        }
        arrayRemaining[stackSize++] = length;
        if (stackSize > maxDepth) {
            throw malformed("reply nesting depth exceeds " + maxDepth);
        }
        return null;
    }

    private BenchmarkRespReply consumeBulkPayload(ByteBuffer input) {
        int consumed = Math.min(input.remaining(), bulkRemaining);
        input.position(input.position() + consumed);
        bulkRemaining -= consumed;
        if (bulkRemaining == 0) {
            stage = Stage.BULK_CR;
        }
        return null;
    }

    private BenchmarkRespReply consumeBulkCr(ByteBuffer input) throws IOException {
        if (input.get() != CR) {
            throw malformed("bulk payload is not followed by CRLF");
        }
        stage = Stage.BULK_LF;
        return null;
    }

    private BenchmarkRespReply consumeBulkLf(ByteBuffer input) throws IOException {
        if (input.get() != LF) {
            throw malformed("bulk payload is not followed by CRLF");
        }
        stage = Stage.MARKER;
        if (currentTopLevel) {
            return BenchmarkRespReply.bulkString(bulkLength);
        }
        return completeDiscardedReply();
    }

    private BenchmarkRespReply completeDiscardedReply() {
        while (stackSize > 0) {
            int top = stackSize - 1;
            int remaining = arrayRemaining[top] - 1;
            arrayRemaining[top] = remaining;
            if (remaining > 0) {
                return null;
            }
            stackSize = top;
        }
        if (rootArrayLength < 0) {
            throw new IllegalStateException("completed child without a root array");
        }
        return BenchmarkRespReply.array(rootArrayLength);
    }

    private void appendLineByte(byte value) throws IOException {
        if (lineLength >= maxLineBytes) {
            throw malformed("line exceeds " + maxLineBytes + " bytes");
        }
        if (lineLength == lineBuffer.length) {
            long doubled = lineBuffer.length == 0 ? 1L : (long) lineBuffer.length * 2L;
            int nextLength = (int) Math.min(maxLineBytes, doubled);
            lineBuffer = Arrays.copyOf(lineBuffer, nextLength);
        }
        lineBuffer[lineLength++] = value;
    }

    private long parseLong() throws IOException {
        if (lineLength == 0) {
            throw malformed("numeric field is empty");
        }

        int index = 0;
        boolean negative = lineBuffer[index] == '-';
        if (negative && ++index == lineLength) {
            throw malformed("numeric field has no digits");
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multiplyLimit = limit / 10;
        long result = 0;
        while (index < lineLength) {
            byte current = lineBuffer[index++];
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

    private void clearState() {
        active = false;
        stage = Stage.MARKER;
        stackSize = 0;
        rootArrayLength = -1;
        currentMarker = 0;
        currentTopLevel = false;
        lineLength = 0;
        bulkLength = 0;
        bulkRemaining = 0;
    }

    private static int requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static int requireDepth(int value) {
        requireNonNegative(value, "maxDepth");
        if (value > MAX_SUPPORTED_DEPTH) {
            throw new IllegalArgumentException(
                    "maxDepth must be <= " + MAX_SUPPORTED_DEPTH
            );
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

    private enum Stage {
        MARKER,
        LINE,
        LINE_LF,
        BULK_PAYLOAD,
        BULK_CR,
        BULK_LF
    }
}
