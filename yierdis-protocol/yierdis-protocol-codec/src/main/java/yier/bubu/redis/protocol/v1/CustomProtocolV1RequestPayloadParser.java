package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.protocol.json.JsonParseException;

import java.util.Arrays;
import java.util.Objects;

/**
 * Request-specific parser for the strict Custom Protocol v1 request payload schema.
 */
public final class CustomProtocolV1RequestPayloadParser {
    private CustomProtocolV1RequestPayloadParser() {
    }

    public static CustomProtocolV1ArgvRequest parse(byte[] payload, int off, int len, int maxArgs) {
        Objects.requireNonNull(payload, "payload");
        if (off < 0 || len < 0 || off + len > payload.length) {
            throw new IndexOutOfBoundsException();
        }
        if (len <= 0) {
            throw new JsonParseException("Invalid JSON");
        }
        return new RequestCursor(payload, off, len, maxArgs).parse();
    }

    private enum FieldName {
        CMD,
        ARGS
    }

    private static final class RequestCursor {
        private final byte[] payload;
        private final int limit;
        private final int maxArgs;

        private int pos;
        private byte[] cmd;
        private byte[][] args = new byte[4][];
        private int argCount;
        private int retainedBytes;
        private boolean seenCmd;
        private boolean seenArgs;

        private RequestCursor(byte[] payload, int off, int len, int maxArgs) {
            this.payload = payload;
            this.pos = off;
            this.limit = off + len;
            this.maxArgs = Math.max(0, maxArgs);
        }

        private CustomProtocolV1ArgvRequest parse() {
            skipWhitespace();
            expect('{');
            skipWhitespace();

            if (tryConsume('}')) {
                throw new IllegalArgumentException("cmd field missing");
            }

            while (true) {
                FieldName field = readFieldName();
                skipWhitespace();
                expect(':');
                skipWhitespace();

                switch (field) {
                    case CMD:
                        readCmdField();
                        break;
                    case ARGS:
                        readArgsField();
                        break;
                    default:
                        throw new IllegalStateException("unexpected field");
                }

                skipWhitespace();
                if (tryConsume(',')) {
                    skipWhitespace();
                    continue;
                }
                expect('}');
                break;
            }

            skipWhitespace();
            if (pos != limit) {
                throw error("Trailing data");
            }
            if (!seenCmd) {
                throw new IllegalArgumentException("cmd field missing");
            }

            byte[][] argv = new byte[1 + argCount][];
            argv[0] = cmd;
            if (argCount > 0) {
                System.arraycopy(args, 0, argv, 1, argCount);
            }
            return CustomProtocolV1ArgvRequest.of(argv, retainedBytes);
        }

        private FieldName readFieldName() {
            expect('"');
            int start = pos;
            while (pos < limit) {
                int b = payload[pos] & 0xFF;
                if (b == '"') {
                    int len = pos - start;
                    pos++;
                    if (matchesAscii(start, len, 'c', 'm', 'd')) {
                        return FieldName.CMD;
                    }
                    if (matchesAscii(start, len, 'a', 'r', 'g', 's')) {
                        return FieldName.ARGS;
                    }
                    throw new IllegalArgumentException("unknown field");
                }
                if (b == '\\') {
                    throw new IllegalArgumentException("unknown field");
                }
                if (b < 0x20) {
                    throw error("Invalid control character in string");
                }
                pos = advanceUtf8(pos);
            }
            throw error("Unexpected EOF");
        }

        private boolean matchesAscii(int start, int len, char... expected) {
            if (len != expected.length) {
                return false;
            }
            for (int i = 0; i < expected.length; i++) {
                if ((payload[start + i] & 0xFF) != expected[i]) {
                    return false;
                }
            }
            return true;
        }

        private void readCmdField() {
            if (seenCmd) {
                throw new IllegalArgumentException("duplicate cmd field");
            }
            if (!hasByte('"')) {
                throw new IllegalArgumentException("cmd must be a string");
            }
            byte[] cmdBytes = readJsonStringUtf8();
            int start = trimAsciiStart(cmdBytes);
            int end = trimAsciiEnd(cmdBytes);
            if (start >= end) {
                throw new IllegalArgumentException("cmd must not be blank");
            }
            cmd = start == 0 && end == cmdBytes.length ? cmdBytes : Arrays.copyOfRange(cmdBytes, start, end);
            retainedBytes += cmd.length;
            if (maxArgs > 0 && 1 + argCount > maxArgs) {
                throw new IllegalArgumentException("too many args");
            }
            seenCmd = true;
        }

        private void readArgsField() {
            if (seenArgs) {
                throw new IllegalArgumentException("duplicate args field");
            }
            if (consumeLiteralIfPresent("null")) {
                seenArgs = true;
                return;
            }
            expect('[');
            skipWhitespace();
            if (tryConsume(']')) {
                seenArgs = true;
                return;
            }
            while (true) {
                if (hasByte('"')) {
                    byte[] arg = readJsonStringUtf8();
                    addArg(arg);
                } else if (consumeLiteralIfPresent("null")) {
                    addArg(null);
                } else {
                    throw new IllegalArgumentException("args elements must be string|null");
                }
                skipWhitespace();
                if (tryConsume(',')) {
                    skipWhitespace();
                    continue;
                }
                expect(']');
                seenArgs = true;
                return;
            }
        }

        private void addArg(byte[] arg) {
            if (maxArgs > 0 && 1 + argCount + 1 > maxArgs) {
                throw new IllegalArgumentException("too many args");
            }
            ensureArgCapacity(argCount + 1);
            args[argCount++] = arg;
            if (arg != null) {
                retainedBytes += arg.length;
            }
        }

        private void ensureArgCapacity(int needed) {
            if (needed <= args.length) {
                return;
            }
            int next = Math.max(needed, args.length * 2);
            args = Arrays.copyOf(args, next);
        }

        private byte[] readJsonStringUtf8() {
            expect('"');
            int segmentStart = pos;
            ByteAccumulator out = null;

            while (pos < limit) {
                int b = payload[pos] & 0xFF;
                if (b == '"') {
                    if (out == null) {
                        byte[] raw = Arrays.copyOfRange(payload, segmentStart, pos);
                        pos++;
                        return raw;
                    }
                    out.append(payload, segmentStart, pos - segmentStart);
                    pos++;
                    return out.toByteArray();
                }
                if (b == '\\') {
                    if (out == null) {
                        out = new ByteAccumulator(Math.max(16, pos - segmentStart + 8));
                    }
                    out.append(payload, segmentStart, pos - segmentStart);
                    pos++;
                    appendEscapedValue(out);
                    segmentStart = pos;
                    continue;
                }
                if (b < 0x20) {
                    throw error("Invalid control character in string");
                }
                pos = advanceUtf8(pos);
            }

            throw error("Unexpected EOF");
        }

        private void appendEscapedValue(ByteAccumulator out) {
            if (pos >= limit) {
                throw error("Unexpected EOF");
            }
            int escape = payload[pos++] & 0xFF;
            switch (escape) {
                case '"':
                case '\\':
                case '/':
                    out.appendByte(escape);
                    return;
                case 'b':
                    out.appendByte('\b');
                    return;
                case 'f':
                    out.appendByte('\f');
                    return;
                case 'n':
                    out.appendByte('\n');
                    return;
                case 'r':
                    out.appendByte('\r');
                    return;
                case 't':
                    out.appendByte('\t');
                    return;
                case 'u':
                    appendUnicodeEscape(out);
                    return;
                default:
                    throw error("Invalid escape sequence");
            }
        }

        private void appendUnicodeEscape(ByteAccumulator out) {
            int codeUnit = readHexQuad();
            if (Character.isHighSurrogate((char) codeUnit)) {
                if (pos + 1 >= limit || payload[pos] != '\\' || payload[pos + 1] != 'u') {
                    throw error("Invalid unicode escape");
                }
                pos += 2;
                int low = readHexQuad();
                if (!Character.isLowSurrogate((char) low)) {
                    throw error("Invalid unicode escape");
                }
                out.appendCodePoint(Character.toCodePoint((char) codeUnit, (char) low));
                return;
            }
            if (Character.isLowSurrogate((char) codeUnit)) {
                throw error("Invalid unicode escape");
            }
            out.appendCodePoint(codeUnit);
        }

        private int readHexQuad() {
            if (pos + 4 > limit) {
                throw error("Unexpected EOF");
            }
            int value = 0;
            for (int i = 0; i < 4; i++) {
                int hex = hexValue(payload[pos + i] & 0xFF);
                if (hex < 0) {
                    throw error("Invalid unicode escape");
                }
                value = (value << 4) | hex;
            }
            pos += 4;
            return value;
        }

        private int hexValue(int b) {
            if (b >= '0' && b <= '9') {
                return b - '0';
            }
            if (b >= 'A' && b <= 'F') {
                return b - 'A' + 10;
            }
            if (b >= 'a' && b <= 'f') {
                return b - 'a' + 10;
            }
            return -1;
        }

        private int advanceUtf8(int index) {
            int b1 = payload[index] & 0xFF;
            if (b1 < 0x80) {
                return index + 1;
            }
            if (b1 < 0xC2) {
                throw error("Invalid UTF-8");
            }
            if (b1 < 0xE0) {
                continuation(index + 1);
                return index + 2;
            }
            if (b1 < 0xF0) {
                int b2 = continuation(index + 1);
                continuation(index + 2);
                if (b1 == 0xE0 && b2 < 0xA0) {
                    throw error("Invalid UTF-8");
                }
                if (b1 == 0xED && b2 >= 0xA0) {
                    throw error("Invalid UTF-8");
                }
                return index + 3;
            }
            if (b1 < 0xF5) {
                int b2 = continuation(index + 1);
                continuation(index + 2);
                continuation(index + 3);
                if (b1 == 0xF0 && b2 < 0x90) {
                    throw error("Invalid UTF-8");
                }
                if (b1 == 0xF4 && b2 >= 0x90) {
                    throw error("Invalid UTF-8");
                }
                return index + 4;
            }
            throw error("Invalid UTF-8");
        }

        private int continuation(int index) {
            if (index >= limit) {
                throw error("Unexpected EOF");
            }
            int b = payload[index] & 0xFF;
            if ((b & 0xC0) != 0x80) {
                throw error("Invalid UTF-8");
            }
            return b;
        }

        private int trimAsciiStart(byte[] bytes) {
            int start = 0;
            while (start < bytes.length && isTrimmedAsciiWhitespace(bytes[start])) {
                start++;
            }
            return start;
        }

        private int trimAsciiEnd(byte[] bytes) {
            int end = bytes.length;
            while (end > 0 && isTrimmedAsciiWhitespace(bytes[end - 1])) {
                end--;
            }
            return end;
        }

        private boolean isTrimmedAsciiWhitespace(byte b) {
            return (b & 0xFF) <= 0x20;
        }

        private boolean consumeLiteralIfPresent(String literal) {
            if (!hasLiteral(literal)) {
                return false;
            }
            pos += literal.length();
            return true;
        }

        private boolean hasLiteral(String literal) {
            if (pos + literal.length() > limit) {
                return false;
            }
            for (int i = 0; i < literal.length(); i++) {
                if ((payload[pos + i] & 0xFF) != literal.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        private boolean hasByte(char expected) {
            return pos < limit && (payload[pos] & 0xFF) == expected;
        }

        private boolean tryConsume(char expected) {
            if (!hasByte(expected)) {
                return false;
            }
            pos++;
            return true;
        }

        private void expect(char expected) {
            if (!tryConsume(expected)) {
                throw error("Expected '" + expected + "'");
            }
        }

        private void skipWhitespace() {
            while (pos < limit) {
                int b = payload[pos] & 0xFF;
                if (b == ' ' || b == '\t' || b == '\n' || b == '\r') {
                    pos++;
                    continue;
                }
                return;
            }
        }

        private JsonParseException error(String message) {
            return new JsonParseException(message);
        }
    }

    private static final class ByteAccumulator {
        private byte[] buf;
        private int len;

        private ByteAccumulator(int initialCapacity) {
            this.buf = new byte[Math.max(16, initialCapacity)];
        }

        private void append(byte[] src, int off, int length) {
            if (length <= 0) {
                return;
            }
            ensureCapacity(len + length);
            System.arraycopy(src, off, buf, len, length);
            len += length;
        }

        private void appendByte(int value) {
            ensureCapacity(len + 1);
            buf[len++] = (byte) value;
        }

        private void appendCodePoint(int codePoint) {
            if (codePoint <= 0x7F) {
                appendByte(codePoint);
                return;
            }
            if (codePoint <= 0x7FF) {
                ensureCapacity(len + 2);
                buf[len++] = (byte) (0xC0 | (codePoint >>> 6));
                buf[len++] = (byte) (0x80 | (codePoint & 0x3F));
                return;
            }
            if (codePoint <= 0xFFFF) {
                ensureCapacity(len + 3);
                buf[len++] = (byte) (0xE0 | (codePoint >>> 12));
                buf[len++] = (byte) (0x80 | ((codePoint >>> 6) & 0x3F));
                buf[len++] = (byte) (0x80 | (codePoint & 0x3F));
                return;
            }
            ensureCapacity(len + 4);
            buf[len++] = (byte) (0xF0 | (codePoint >>> 18));
            buf[len++] = (byte) (0x80 | ((codePoint >>> 12) & 0x3F));
            buf[len++] = (byte) (0x80 | ((codePoint >>> 6) & 0x3F));
            buf[len++] = (byte) (0x80 | (codePoint & 0x3F));
        }

        private void ensureCapacity(int needed) {
            if (needed <= buf.length) {
                return;
            }
            int next = Math.max(needed, buf.length * 2);
            buf = Arrays.copyOf(buf, next);
        }

        private byte[] toByteArray() {
            return Arrays.copyOf(buf, len);
        }
    }
}
