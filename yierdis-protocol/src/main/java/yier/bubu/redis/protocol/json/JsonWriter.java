package yier.bubu.redis.protocol.json;

import yier.bubu.redis.bytes.BytesSink;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Minimal JSON writer for the custom protocol.
 * <p>
 * Output is always a single line (no pretty printing, no unescaped control characters).
 */
public final class JsonWriter {
    private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TRUE = "true".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSE = "false".getBytes(StandardCharsets.US_ASCII);

    private JsonWriter() {
    }

    public static void writeValue(BytesSink out, JsonValue value) {
        Objects.requireNonNull(out, "out");
        if (value == null || value instanceof JsonNull) {
            out.writeBytes(NULL, 0, NULL.length);
            return;
        }
        if (value instanceof JsonBoolean b) {
            byte[] raw = b.value() ? TRUE : FALSE;
            out.writeBytes(raw, 0, raw.length);
            return;
        }
        if (value instanceof JsonString s) {
            writeString(out, s.value());
            return;
        }
        if (value instanceof JsonLong n) {
            writeAscii(out, Long.toString(n.value()));
            return;
        }
        if (value instanceof JsonDouble n) {
            writeAscii(out, Double.toString(n.value()));
            return;
        }
        if (value instanceof JsonArray arr) {
            writeArray(out, arr.values());
            return;
        }
        if (value instanceof JsonObject obj) {
            writeObject(out, obj.values());
            return;
        }
        throw new IllegalArgumentException("Unknown JsonValue: " + value.getClass().getName());
    }

    public static void writeString(BytesSink out, String s) {
        Objects.requireNonNull(out, "out");
        if (s == null) {
            out.writeBytes(NULL, 0, NULL.length);
            return;
        }

        out.writeBytes(new byte[]{'"'}, 0, 1);
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    out.writeBytes(new byte[]{'\\', '"'}, 0, 2);
                    continue;
                case '\\':
                    out.writeBytes(new byte[]{'\\', '\\'}, 0, 2);
                    continue;
                case '\b':
                    out.writeBytes(new byte[]{'\\', 'b'}, 0, 2);
                    continue;
                case '\f':
                    out.writeBytes(new byte[]{'\\', 'f'}, 0, 2);
                    continue;
                case '\n':
                    out.writeBytes(new byte[]{'\\', 'n'}, 0, 2);
                    continue;
                case '\r':
                    out.writeBytes(new byte[]{'\\', 'r'}, 0, 2);
                    continue;
                case '\t':
                    out.writeBytes(new byte[]{'\\', 't'}, 0, 2);
                    continue;
                default:
                    break;
            }
            if (c <= 0x1F) {
                writeAscii(out, "\\u00" + hex2(c));
                continue;
            }
            writeUtf8Char(out, c, s, len, i);
            if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(s.charAt(i + 1))) {
                i++;
            }
        }
        out.writeBytes(new byte[]{'"'}, 0, 1);
    }

    private static String hex2(int c) {
        String h = Integer.toHexString(c & 0xFF).toUpperCase(java.util.Locale.ROOT);
        return h.length() == 1 ? "0" + h : h;
    }

    private static void writeArray(BytesSink out, List<JsonValue> values) {
        out.writeBytes(new byte[]{'['}, 0, 1);
        if (values != null && !values.isEmpty()) {
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    out.writeBytes(new byte[]{','}, 0, 1);
                }
                writeValue(out, values.get(i));
            }
        }
        out.writeBytes(new byte[]{']'}, 0, 1);
    }

    private static void writeObject(BytesSink out, Map<String, JsonValue> values) {
        out.writeBytes(new byte[]{'{'}, 0, 1);
        if (values != null && !values.isEmpty()) {
            boolean first = true;
            for (Map.Entry<String, JsonValue> e : values.entrySet()) {
                if (!first) {
                    out.writeBytes(new byte[]{','}, 0, 1);
                }
                first = false;
                writeString(out, e.getKey());
                out.writeBytes(new byte[]{':'}, 0, 1);
                writeValue(out, e.getValue());
            }
        }
        out.writeBytes(new byte[]{'}'}, 0, 1);
    }

    private static void writeAscii(BytesSink out, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.US_ASCII);
        out.writeBytes(bytes, 0, bytes.length);
    }

    private static void writeUtf8Char(BytesSink out, char c, String s, int len, int i) {
        int codePoint;
        if (Character.isHighSurrogate(c) && i + 1 < len) {
            char low = s.charAt(i + 1);
            if (Character.isLowSurrogate(low)) {
                codePoint = Character.toCodePoint(c, low);
            } else {
                codePoint = c;
            }
        } else {
            codePoint = c;
        }

        byte[] tmp = new byte[4];
        int n = 0;
        if (codePoint <= 0x7F) {
            tmp[n++] = (byte) codePoint;
        } else if (codePoint <= 0x7FF) {
            tmp[n++] = (byte) (0xC0 | ((codePoint >>> 6) & 0x1F));
            tmp[n++] = (byte) (0x80 | (codePoint & 0x3F));
        } else if (codePoint <= 0xFFFF) {
            tmp[n++] = (byte) (0xE0 | ((codePoint >>> 12) & 0x0F));
            tmp[n++] = (byte) (0x80 | ((codePoint >>> 6) & 0x3F));
            tmp[n++] = (byte) (0x80 | (codePoint & 0x3F));
        } else {
            tmp[n++] = (byte) (0xF0 | ((codePoint >>> 18) & 0x07));
            tmp[n++] = (byte) (0x80 | ((codePoint >>> 12) & 0x3F));
            tmp[n++] = (byte) (0x80 | ((codePoint >>> 6) & 0x3F));
            tmp[n++] = (byte) (0x80 | (codePoint & 0x3F));
        }
        out.writeBytes(tmp, 0, n);
    }
}

