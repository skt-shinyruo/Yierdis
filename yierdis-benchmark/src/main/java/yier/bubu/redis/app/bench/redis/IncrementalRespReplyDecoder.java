package yier.bubu.redis.app.bench.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class IncrementalRespReplyDecoder {
    static final int MAX_SUPPORTED_DEPTH = 4096;

    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final int PREFIX_ANCHOR_BYTES = Long.BYTES;

    private final int maxBulkBytes;
    private final int maxLineBytes;
    private final int maxArrayLength;
    private final int maxDepth;
    private final int[] arrayRemaining;

    private boolean active;
    private Stage stage = Stage.MARKER;
    private int cursor;
    private int stackSize;
    private int rootArrayLength = -1;

    private byte currentMarker;
    private boolean currentTopLevel;
    private int lineStart;
    private int lineScanOffset;

    private int bulkLength;
    private int bulkPayloadStart;

    private int retainedInputLength;
    private long retainedHeadAnchor;
    private long retainedTailAnchor;
    private ByteBuffer retainedBuffer;
    private int retainedBufferStart;
    private long prefixValidationByteReads;

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
    }

    public BenchmarkRespReply tryDecode(ByteBuffer input) throws IOException {
        Objects.requireNonNull(input, "input");
        int start = input.position();
        int available = input.limit() - start;
        try {
            if (!active) {
                startReply();
            } else {
                validateRetainedPrefix(input, start, available);
            }

            BenchmarkRespReply reply = advance(input, start, available);
            if (reply == null) {
                retainInput(input, start, available);
                input.position(start);
                return null;
            }

            int consumed = cursor;
            clearState();
            input.position(start + consumed);
            return reply;
        } catch (IOException failure) {
            clearState();
            throw failure;
        }
    }

    int retainedProgress() {
        if (!active) {
            return 0;
        }
        return switch (stage) {
            case MARKER -> cursor;
            case LINE -> lineScanOffset;
            case BULK -> bulkPayloadStart;
        };
    }

    int retainedArrayDepth() {
        return active ? stackSize : 0;
    }

    long prefixValidationByteReads() {
        return prefixValidationByteReads;
    }

    private BenchmarkRespReply advance(ByteBuffer input, int start, int available) throws IOException {
        while (true) {
            if (stage == Stage.MARKER) {
                if (stackSize > maxDepth) {
                    throw malformed("reply nesting depth exceeds " + maxDepth);
                }
                if (cursor == available) {
                    return null;
                }

                currentMarker = input.get(start + cursor++);
                if (currentMarker != '+' && currentMarker != '-' && currentMarker != ':'
                        && currentMarker != '$' && currentMarker != '*') {
                    throw malformed("unsupported reply marker " + printable(currentMarker));
                }
                currentTopLevel = stackSize == 0;
                lineStart = cursor;
                lineScanOffset = cursor;
                stage = Stage.LINE;
            }

            if (stage == Stage.LINE) {
                int lineEnd = scanLine(input, start, available);
                if (lineEnd < 0) {
                    return null;
                }
                BenchmarkRespReply reply = processLine(input, start, lineEnd);
                if (reply != null) {
                    return reply;
                }
                continue;
            }

            BenchmarkRespReply reply = processBulk(input, start, available);
            if (reply != null) {
                return reply;
            }
            if (stage == Stage.BULK) {
                return null;
            }
        }
    }

    private int scanLine(ByteBuffer input, int start, int available) throws IOException {
        while (lineScanOffset < available) {
            byte current = input.get(start + lineScanOffset);
            if (current == CR) {
                if (lineScanOffset + 1 == available) {
                    return -1;
                }
                if (input.get(start + lineScanOffset + 1) != LF) {
                    throw malformed("CR is not followed by LF");
                }
                int lineEnd = lineScanOffset;
                cursor = lineScanOffset + 2;
                stage = Stage.MARKER;
                return lineEnd;
            }
            if (current == LF) {
                throw malformed("LF is not preceded by CR");
            }
            if (lineScanOffset - lineStart >= maxLineBytes) {
                throw malformed("line exceeds " + maxLineBytes + " bytes");
            }
            lineScanOffset++;
        }
        return -1;
    }

    private BenchmarkRespReply processLine(ByteBuffer input, int start, int lineEnd) throws IOException {
        return switch (currentMarker) {
            case '+' -> processText(input, start, lineEnd, false);
            case '-' -> processText(input, start, lineEnd, true);
            case ':' -> processInteger(input, start, lineEnd);
            case '$' -> processBulkLength(input, start, lineEnd);
            case '*' -> processArrayLength(input, start, lineEnd);
            default -> throw new IllegalStateException("validated marker became invalid");
        };
    }

    private BenchmarkRespReply processText(ByteBuffer input, int start, int lineEnd, boolean error) {
        if (currentTopLevel) {
            String text = decodeText(input, start + lineStart, lineEnd - lineStart);
            return error ? BenchmarkRespReply.error(text) : BenchmarkRespReply.simpleString(text);
        }
        return completeDiscardedReply();
    }

    private BenchmarkRespReply processInteger(ByteBuffer input, int start, int lineEnd) throws IOException {
        long value = parseLong(input, start, lineStart, lineEnd);
        if (currentTopLevel) {
            return BenchmarkRespReply.integer(value);
        }
        return completeDiscardedReply();
    }

    private BenchmarkRespReply processBulkLength(ByteBuffer input, int start, int lineEnd) throws IOException {
        long declaredLength = parseLong(input, start, lineStart, lineEnd);
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
        bulkPayloadStart = cursor;
        stage = Stage.BULK;
        return null;
    }

    private BenchmarkRespReply processArrayLength(ByteBuffer input, int start, int lineEnd) throws IOException {
        long declaredLength = parseLong(input, start, lineStart, lineEnd);
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
        return null;
    }

    private BenchmarkRespReply processBulk(ByteBuffer input, int start, int available) throws IOException {
        long payloadEndValue = (long) bulkPayloadStart + bulkLength;
        if (payloadEndValue > available) {
            return null;
        }

        int payloadEnd = (int) payloadEndValue;
        if (payloadEnd == available) {
            return null;
        }
        if (input.get(start + payloadEnd) != CR) {
            throw malformed("bulk payload is not followed by CRLF");
        }
        if (payloadEnd + 1 == available) {
            return null;
        }
        if (input.get(start + payloadEnd + 1) != LF) {
            throw malformed("bulk payload is not followed by CRLF");
        }

        cursor = payloadEnd + 2;
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

    private long parseLong(ByteBuffer input, int start, int valueStart, int valueEnd) throws IOException {
        if (valueStart == valueEnd) {
            throw malformed("numeric field is empty");
        }

        int index = valueStart;
        boolean negative = input.get(start + index) == '-';
        if (negative && ++index == valueEnd) {
            throw malformed("numeric field has no digits");
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multiplyLimit = limit / 10;
        long result = 0;
        while (index < valueEnd) {
            byte current = input.get(start + index++);
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

    private void validateRetainedPrefix(ByteBuffer input, int start, int available) throws IOException {
        if (available < retainedInputLength) {
            throw malformed("input no longer contains retained prefix");
        }
        if (input == retainedBuffer && start == retainedBufferStart) {
            return;
        }

        int headEnd = Math.min(retainedInputLength, PREFIX_ANCHOR_BYTES);
        int tailStart = Math.max(0, retainedInputLength - PREFIX_ANCHOR_BYTES);
        long suppliedHeadAnchor = readAnchor(input, start, 0, headEnd);
        prefixValidationByteReads += headEnd;
        if (suppliedHeadAnchor != retainedHeadAnchor) {
            throw malformed("input no longer contains retained prefix");
        }
        long suppliedTailAnchor = readAnchor(input, start, tailStart, retainedInputLength);
        prefixValidationByteReads += retainedInputLength - tailStart;
        if (suppliedTailAnchor != retainedTailAnchor) {
            throw malformed("input no longer contains retained prefix");
        }
    }

    private void retainInput(ByteBuffer input, int start, int available) {
        retainedInputLength = available;
        int headEnd = Math.min(available, PREFIX_ANCHOR_BYTES);
        int tailStart = Math.max(0, available - PREFIX_ANCHOR_BYTES);
        retainedHeadAnchor = readAnchor(input, start, 0, headEnd);
        retainedTailAnchor = readAnchor(input, start, tailStart, available);
        retainedBuffer = input;
        retainedBufferStart = start;
    }

    private static long readAnchor(ByteBuffer input, int start, int from, int to) {
        long anchor = 0;
        for (int offset = from; offset < to; offset++) {
            anchor = (anchor << Byte.SIZE) | (input.get(start + offset) & 0xffL);
        }
        return anchor;
    }

    private static String decodeText(ByteBuffer input, int start, int length) {
        byte[] bytes = new byte[length];
        ByteBuffer view = input.duplicate();
        view.position(start);
        view.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void startReply() {
        active = true;
        stage = Stage.MARKER;
        cursor = 0;
        stackSize = 0;
        rootArrayLength = -1;
        retainedInputLength = 0;
        retainedHeadAnchor = 0;
        retainedTailAnchor = 0;
        retainedBuffer = null;
        retainedBufferStart = 0;
    }

    private void clearState() {
        active = false;
        stage = Stage.MARKER;
        cursor = 0;
        stackSize = 0;
        rootArrayLength = -1;
        retainedInputLength = 0;
        retainedHeadAnchor = 0;
        retainedTailAnchor = 0;
        retainedBuffer = null;
        retainedBufferStart = 0;
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
        BULK
    }
}
