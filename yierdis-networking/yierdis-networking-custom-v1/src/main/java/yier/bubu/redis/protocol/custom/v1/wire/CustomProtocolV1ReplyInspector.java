package yier.bubu.redis.protocol.custom.v1.wire;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Lightweight inspector for canonical top-level ok envelopes emitted by Custom Protocol v1.
 * <p>
 * This is intentionally narrower than {@link CustomProtocolV1ReplyParser}: it avoids cloning
 * lines or building detached JSON trees so hot-path tools such as the benchmark can validate
 * reply shapes without importing client-facing allocation costs into measurements.
 */
public final class CustomProtocolV1ReplyInspector {
    public static final int NULL_RESULT = -1;
    public static final int INVALID_RESULT = -2;

    private static final byte[] OK_PREFIX = "{\"ok\":true,\"result\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] B64_PREFIX = "{\"$b64\":\"".getBytes(StandardCharsets.US_ASCII);

    private CustomProtocolV1ReplyInspector() {
    }

    public static boolean matchesOkAsciiStringResult(byte[] line, int off, int len, byte[] expectedAscii) {
        Objects.requireNonNull(line, "line");
        Objects.requireNonNull(expectedAscii, "expectedAscii");
        Slice slice = normalizedSlice(line, off, len);
        int expectedLen = OK_PREFIX.length + 1 + expectedAscii.length + 2;
        if (slice.length() != expectedLen) {
            return false;
        }
        if (!startsWith(line, slice.start(), OK_PREFIX)) {
            return false;
        }
        int pos = slice.start() + OK_PREFIX.length;
        if (line[pos++] != '"') {
            return false;
        }
        for (int i = 0; i < expectedAscii.length; i++) {
            if (line[pos + i] != expectedAscii[i]) {
                return false;
            }
        }
        pos += expectedAscii.length;
        return line[pos] == '"' && line[pos + 1] == '}';
    }

    public static int decodedOkResultByteLength(byte[] line, int off, int len) {
        Objects.requireNonNull(line, "line");
        Slice slice = normalizedSlice(line, off, len);
        if (slice.length() <= OK_PREFIX.length || !startsWith(line, slice.start(), OK_PREFIX)) {
            return INVALID_RESULT;
        }

        int resultStart = slice.start() + OK_PREFIX.length;
        int end = slice.endExclusive();
        byte first = line[resultStart];
        if (first == 'n') {
            return isNullResult(line, resultStart, end) ? NULL_RESULT : INVALID_RESULT;
        }
        if (first == '"') {
            return decodedJsonStringByteLength(line, resultStart + 1, end);
        }
        if (first == '{') {
            return decodeTaggedB64ByteLength(line, resultStart, end);
        }
        return INVALID_RESULT;
    }

    private static boolean isNullResult(byte[] line, int resultStart, int end) {
        return end - resultStart == 5
                && line[resultStart] == 'n'
                && line[resultStart + 1] == 'u'
                && line[resultStart + 2] == 'l'
                && line[resultStart + 3] == 'l'
                && line[resultStart + 4] == '}';
    }

    private static int decodedJsonStringByteLength(byte[] line, int pos, int end) {
        int decodedLen = 0;
        while (pos < end) {
            int b = line[pos++] & 0xFF;
            if (b == '"') {
                return pos == end - 1 && line[pos] == '}' ? decodedLen : INVALID_RESULT;
            }
            if (b == '\\') {
                if (pos >= end) {
                    return INVALID_RESULT;
                }
                int esc = line[pos++] & 0xFF;
                switch (esc) {
                    case '"':
                    case '\\':
                    case '/':
                    case 'b':
                    case 'f':
                    case 'n':
                    case 'r':
                    case 't':
                        decodedLen += 1;
                        continue;
                    case 'u': {
                        UnicodeDecodeResult decoded = decodeUnicodeEscape(line, pos, end);
                        if (!decoded.valid()) {
                            return INVALID_RESULT;
                        }
                        decodedLen += decoded.utf8Length();
                        pos = decoded.nextPos();
                        continue;
                    }
                    default:
                        return INVALID_RESULT;
                }
            }
            if (b < 0x20) {
                return INVALID_RESULT;
            }

            int utf8Len = utf8SequenceLength(line, pos - 1, end);
            if (utf8Len == INVALID_RESULT) {
                return INVALID_RESULT;
            }
            decodedLen += utf8Len;
            pos += utf8Len - 1;
        }
        return INVALID_RESULT;
    }

    private static UnicodeDecodeResult decodeUnicodeEscape(byte[] line, int pos, int end) {
        if (pos + 4 > end) {
            return UnicodeDecodeResult.invalid();
        }
        int codeUnit = decodeHex(line, pos, 4);
        if (codeUnit < 0) {
            return UnicodeDecodeResult.invalid();
        }
        pos += 4;

        if (Character.isHighSurrogate((char) codeUnit)) {
            if (pos + 6 > end || line[pos] != '\\' || line[pos + 1] != 'u') {
                return UnicodeDecodeResult.invalid();
            }
            int low = decodeHex(line, pos + 2, 4);
            if (low < 0 || !Character.isLowSurrogate((char) low)) {
                return UnicodeDecodeResult.invalid();
            }
            int codePoint = Character.toCodePoint((char) codeUnit, (char) low);
            return new UnicodeDecodeResult(true, pos + 6, utf8LengthOfCodePoint(codePoint));
        }
        if (Character.isLowSurrogate((char) codeUnit)) {
            return UnicodeDecodeResult.invalid();
        }
        return new UnicodeDecodeResult(true, pos, utf8LengthOfCodePoint(codeUnit));
    }

    private static int decodeTaggedB64ByteLength(byte[] line, int resultStart, int end) {
        if (end - resultStart < B64_PREFIX.length + 3 || !startsWith(line, resultStart, B64_PREFIX)) {
            return INVALID_RESULT;
        }
        int pos = resultStart + B64_PREFIX.length;
        int dataChars = 0;
        int padding = 0;
        boolean paddingStarted = false;
        while (pos < end) {
            int b = line[pos++] & 0xFF;
            if (b == '"') {
                if (pos + 2 != end || line[pos] != '}' || line[pos + 1] != '}') {
                    return INVALID_RESULT;
                }
                if ((dataChars & 0x03) != 0) {
                    return INVALID_RESULT;
                }
                return ((dataChars / 4) * 3) - padding;
            }
            if (isBase64Byte(b)) {
                if (paddingStarted) {
                    return INVALID_RESULT;
                }
                dataChars++;
                continue;
            }
            if (b == '=') {
                paddingStarted = true;
                padding++;
                dataChars++;
                if (padding > 2) {
                    return INVALID_RESULT;
                }
                continue;
            }
            return INVALID_RESULT;
        }
        return INVALID_RESULT;
    }

    private static boolean isBase64Byte(int b) {
        return (b >= 'A' && b <= 'Z')
                || (b >= 'a' && b <= 'z')
                || (b >= '0' && b <= '9')
                || b == '+'
                || b == '/';
    }

    private static int decodeHex(byte[] line, int pos, int length) {
        int value = 0;
        for (int i = 0; i < length; i++) {
            if (pos + i >= line.length) {
                return -1;
            }
            int digit = Character.digit((char) line[pos + i], 16);
            if (digit < 0) {
                return -1;
            }
            value = (value << 4) | digit;
        }
        return value;
    }

    private static int utf8LengthOfCodePoint(int codePoint) {
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

    private static int utf8SequenceLength(byte[] line, int pos, int end) {
        int b = line[pos] & 0xFF;
        if ((b & 0x80) == 0) {
            return 1;
        }
        if (b >= 0xC2 && b <= 0xDF) {
            return hasContinuation(line, pos, end, 1) ? 2 : INVALID_RESULT;
        }
        if (b >= 0xE0 && b <= 0xEF) {
            if (!hasContinuation(line, pos, end, 2)) {
                return INVALID_RESULT;
            }
            int b1 = line[pos + 1] & 0xFF;
            if (b == 0xE0 && b1 < 0xA0) {
                return INVALID_RESULT;
            }
            if (b == 0xED && b1 >= 0xA0) {
                return INVALID_RESULT;
            }
            return 3;
        }
        if (b >= 0xF0 && b <= 0xF4) {
            if (!hasContinuation(line, pos, end, 3)) {
                return INVALID_RESULT;
            }
            int b1 = line[pos + 1] & 0xFF;
            if (b == 0xF0 && b1 < 0x90) {
                return INVALID_RESULT;
            }
            if (b == 0xF4 && b1 >= 0x90) {
                return INVALID_RESULT;
            }
            return 4;
        }
        return INVALID_RESULT;
    }

    private static boolean hasContinuation(byte[] line, int pos, int end, int count) {
        if (pos + count >= end) {
            return false;
        }
        for (int i = 1; i <= count; i++) {
            int next = line[pos + i] & 0xFF;
            if ((next & 0xC0) != 0x80) {
                return false;
            }
        }
        return true;
    }

    private static boolean startsWith(byte[] line, int pos, byte[] prefix) {
        if (pos < 0 || pos + prefix.length > line.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (line[pos + i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static Slice normalizedSlice(byte[] line, int off, int len) {
        if (off < 0 || len < 0 || off + len > line.length) {
            throw new IndexOutOfBoundsException();
        }
        int end = off + len;
        while (end > off && (line[end - 1] == '\n' || line[end - 1] == '\r')) {
            end--;
        }
        return new Slice(off, end);
    }

    private record Slice(int start, int endExclusive) {
        int length() {
            return endExclusive - start;
        }
    }

    private record UnicodeDecodeResult(boolean valid, int nextPos, int utf8Length) {
        static UnicodeDecodeResult invalid() {
            return new UnicodeDecodeResult(false, -1, 0);
        }
    }
}
