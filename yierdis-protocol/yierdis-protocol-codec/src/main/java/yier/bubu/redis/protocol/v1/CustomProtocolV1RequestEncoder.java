package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.bytes.BytesSink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Custom Protocol v1 request framing helper shared by client-side tools.
 */
public final class CustomProtocolV1RequestEncoder {
    private static final int MAX_INT_ASCII_BYTES = 10;
    private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FRAME_PREFIX_CMD = "{\"cmd\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FRAME_ARGS_PREFIX = ",\"args\":[".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FRAME_SUFFIX = "]}".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] COMMA = ",".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] COLON = ":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] QUOTE = "\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ESCAPED_QUOTE = "\\\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ESCAPED_BACKSLASH = "\\\\".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ESCAPED_BACKSPACE = "\\b".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ESCAPED_FORM_FEED = "\\f".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ESCAPED_NEWLINE = "\\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ESCAPED_CARRIAGE_RETURN = "\\r".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ESCAPED_TAB = "\\t".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] UNICODE_ESCAPE_PREFIX = "\\u00".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEX = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    private CustomProtocolV1RequestEncoder() {
    }

    public static byte[] encodeRequestFrame(List<byte[]> args) {
        ByteArrayOutputStream frame = new ByteArrayOutputStream(64);
        try {
            writeNormalizedRequestFrame(frame, args);
        } catch (IOException e) {
            throw new IllegalStateException("unexpected in-memory write failure", e);
        }
        return frame.toByteArray();
    }

    public static void writeRequestFrame(OutputStream out, List<byte[]> args) throws IOException {
        Objects.requireNonNull(out, "out");
        writeNormalizedRequestFrame(out, args);
    }

    private static void writeNormalizedRequestFrame(OutputStream out, List<byte[]> args) throws IOException {
        try {
            // Client-facing frame APIs must always emit valid JSON UTF-8, even for malformed raw bytes.
            writeRequestFrame(new OutputStreamBytesSink(out), normalizeUtf8Args(args), new byte[16]);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    public static void writeRequestFrame(BytesSink sink, List<byte[]> args, byte[] asciiIntBuffer) {
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(asciiIntBuffer, "asciiIntBuffer");
        if (asciiIntBuffer.length < MAX_INT_ASCII_BYTES) {
            throw new IllegalArgumentException("asciiIntBuffer must be at least 10 bytes");
        }
        if (args == null || args.isEmpty()) {
            throw new IllegalArgumentException("args must not be empty");
        }

        List<byte[]> normalizedArgs = normalizeUtf8ArgsIfNeeded(args);

        byte[] cmdBytes = normalizedArgs.get(0);
        if (cmdBytes == null || cmdBytes.length == 0) {
            throw new IllegalArgumentException("command name must not be null/empty");
        }
        int cmdStart = trimAsciiStart(cmdBytes);
        int cmdEnd = trimAsciiEnd(cmdBytes);
        if (cmdStart >= cmdEnd) {
            throw new IllegalArgumentException("command name must not be blank");
        }

        int payloadLen = measurePayloadLength(normalizedArgs, cmdBytes, cmdStart, cmdEnd);

        writeIntAscii(sink, payloadLen, asciiIntBuffer);
        sink.writeBytes(COLON, 0, COLON.length);
        writePayload(sink, normalizedArgs, cmdBytes, cmdStart, cmdEnd);
        sink.writeBytes(NEWLINE, 0, NEWLINE.length);
    }

    private static int trimAsciiStart(byte[] bytes) {
        int start = 0;
        while (start < bytes.length && isTrimmedAsciiWhitespace(bytes[start])) {
            start++;
        }
        return start;
    }

    private static int trimAsciiEnd(byte[] bytes) {
        int end = bytes.length;
        while (end > 0 && isTrimmedAsciiWhitespace(bytes[end - 1])) {
            end--;
        }
        return end;
    }

    private static boolean isTrimmedAsciiWhitespace(byte b) {
        return (b & 0xFF) <= 0x20;
    }

    private static int measurePayloadLength(List<byte[]> args, byte[] cmdBytes, int cmdStart, int cmdEnd) {
        int total = FRAME_PREFIX_CMD.length
                + jsonStringEncodedLength(cmdBytes, cmdStart, cmdEnd)
                + FRAME_ARGS_PREFIX.length
                + FRAME_SUFFIX.length;

        for (int i = 1; i < args.size(); i++) {
            if (i > 1) {
                total += COMMA.length;
            }
            byte[] arg = args.get(i);
            total += arg == null ? NULL.length : jsonStringEncodedLength(arg, 0, arg.length);
        }
        return total;
    }

    private static void writePayload(BytesSink sink, List<byte[]> args, byte[] cmdBytes, int cmdStart, int cmdEnd) {
        sink.writeBytes(FRAME_PREFIX_CMD, 0, FRAME_PREFIX_CMD.length);
        writeJsonUtf8String(sink, cmdBytes, cmdStart, cmdEnd);
        sink.writeBytes(FRAME_ARGS_PREFIX, 0, FRAME_ARGS_PREFIX.length);
        for (int i = 1; i < args.size(); i++) {
            if (i > 1) {
                sink.writeBytes(COMMA, 0, COMMA.length);
            }
            byte[] arg = args.get(i);
            if (arg == null) {
                sink.writeBytes(NULL, 0, NULL.length);
            } else {
                writeJsonUtf8String(sink, arg, 0, arg.length);
            }
        }
        sink.writeBytes(FRAME_SUFFIX, 0, FRAME_SUFFIX.length);
    }

    private static int jsonStringEncodedLength(byte[] utf8, int start, int end) {
        int total = QUOTE.length + QUOTE.length;
        for (int i = start; i < end; i++) {
            int b = utf8[i] & 0xFF;
            total += escapeLength(b);
        }
        return total;
    }

    private static int escapeLength(int b) {
        switch (b) {
            case '"':
            case '\\':
            case '\b':
            case '\f':
            case '\n':
            case '\r':
            case '\t':
                return 2;
            default:
                return b < 0x20 ? 6 : 1;
        }
    }

    private static void writeJsonUtf8String(BytesSink sink, byte[] utf8, int start, int end) {
        sink.writeBytes(QUOTE, 0, QUOTE.length);
        int chunkStart = start;
        for (int i = start; i < end; i++) {
            int b = utf8[i] & 0xFF;
            byte[] escape = simpleEscapeBytes(b);
            if (escape == null && b >= 0x20) {
                continue;
            }
            if (i > chunkStart) {
                sink.writeBytes(utf8, chunkStart, i - chunkStart);
            }
            if (escape != null) {
                sink.writeBytes(escape, 0, escape.length);
            } else {
                writeUnicodeEscape(sink, b);
            }
            chunkStart = i + 1;
        }
        if (end > chunkStart) {
            sink.writeBytes(utf8, chunkStart, end - chunkStart);
        }
        sink.writeBytes(QUOTE, 0, QUOTE.length);
    }

    private static byte[] simpleEscapeBytes(int b) {
        switch (b) {
            case '"':
                return ESCAPED_QUOTE;
            case '\\':
                return ESCAPED_BACKSLASH;
            case '\b':
                return ESCAPED_BACKSPACE;
            case '\f':
                return ESCAPED_FORM_FEED;
            case '\n':
                return ESCAPED_NEWLINE;
            case '\r':
                return ESCAPED_CARRIAGE_RETURN;
            case '\t':
                return ESCAPED_TAB;
            default:
                return null;
        }
    }

    private static void writeUnicodeEscape(BytesSink sink, int b) {
        sink.writeBytes(UNICODE_ESCAPE_PREFIX, 0, UNICODE_ESCAPE_PREFIX.length);
        sink.writeBytes(HEX, (b >>> 4) & 0x0F, 1);
        sink.writeBytes(HEX, b & 0x0F, 1);
    }

    private static void writeIntAscii(BytesSink sink, int value, byte[] asciiIntBuffer) {
        int v = Math.max(0, value);
        int pos = asciiIntBuffer.length;
        if (v == 0) {
            asciiIntBuffer[--pos] = '0';
        } else {
            while (v > 0) {
                asciiIntBuffer[--pos] = (byte) ('0' + (v % 10));
                v /= 10;
            }
        }
        sink.writeBytes(asciiIntBuffer, pos, asciiIntBuffer.length - pos);
    }

    private static List<byte[]> normalizeUtf8Args(List<byte[]> args) {
        if (args == null) {
            return null;
        }
        List<byte[]> normalized = new ArrayList<>(args.size());
        for (byte[] arg : args) {
            normalized.add(normalizeUtf8(arg));
        }
        return normalized;
    }

    private static List<byte[]> normalizeUtf8ArgsIfNeeded(List<byte[]> args) {
        for (byte[] arg : args) {
            if (arg != null && !isStrictUtf8(arg)) {
                return normalizeUtf8Args(args);
            }
        }
        return args;
    }

    private static byte[] normalizeUtf8(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
    }

    private static boolean isStrictUtf8(byte[] bytes) {
        int remaining = 0;
        boolean firstContinuation = false;
        int firstMin = 0x80;
        int firstMax = 0xBF;

        for (byte value : bytes) {
            int b = value & 0xFF;

            if (b < 0x80) {
                if (remaining != 0) {
                    return false;
                }
                continue;
            }

            if (remaining == 0) {
                if (b >= 0xC2 && b <= 0xDF) {
                    remaining = 1;
                    firstContinuation = true;
                    firstMin = 0x80;
                    firstMax = 0xBF;
                } else if (b == 0xE0) {
                    remaining = 2;
                    firstContinuation = true;
                    firstMin = 0xA0;
                    firstMax = 0xBF;
                } else if (b >= 0xE1 && b <= 0xEC) {
                    remaining = 2;
                    firstContinuation = true;
                    firstMin = 0x80;
                    firstMax = 0xBF;
                } else if (b == 0xED) {
                    remaining = 2;
                    firstContinuation = true;
                    firstMin = 0x80;
                    firstMax = 0x9F;
                } else if (b >= 0xEE && b <= 0xEF) {
                    remaining = 2;
                    firstContinuation = true;
                    firstMin = 0x80;
                    firstMax = 0xBF;
                } else if (b == 0xF0) {
                    remaining = 3;
                    firstContinuation = true;
                    firstMin = 0x90;
                    firstMax = 0xBF;
                } else if (b >= 0xF1 && b <= 0xF3) {
                    remaining = 3;
                    firstContinuation = true;
                    firstMin = 0x80;
                    firstMax = 0xBF;
                } else if (b == 0xF4) {
                    remaining = 3;
                    firstContinuation = true;
                    firstMin = 0x80;
                    firstMax = 0x8F;
                } else {
                    return false;
                }
                continue;
            }

            if (firstContinuation) {
                if (b < firstMin || b > firstMax) {
                    return false;
                }
                firstContinuation = false;
            } else if (b < 0x80 || b > 0xBF) {
                return false;
            }

            remaining--;
            if (remaining == 0) {
                firstContinuation = false;
            }
        }

        return remaining == 0;
    }

    private static final class OutputStreamBytesSink implements BytesSink {
        private final OutputStream out;

        private OutputStreamBytesSink(OutputStream out) {
            this.out = out;
        }

        @Override
        public void writeBytes(byte[] src, int srcIndex, int len) {
            try {
                out.write(src, srcIndex, len);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
