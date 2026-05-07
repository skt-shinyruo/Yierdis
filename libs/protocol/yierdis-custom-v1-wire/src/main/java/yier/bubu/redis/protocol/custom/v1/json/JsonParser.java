package yier.bubu.redis.protocol.custom.v1.json;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON parser for the custom protocol.
 * <p>
 * Supports: object/array/string/number/bool/null. Does not support comments or trailing commas.
 */
public final class JsonParser {
    private static final ThreadLocal<CharsetDecoder> TL_UTF8_DECODER = ThreadLocal.withInitial(() -> StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT));

    private JsonParser() {
    }

    public static JsonValue parseStrictUtf8(byte[] utf8, int off, int len, JsonLimits limits) {
        if (utf8 == null) {
            throw new IllegalArgumentException("utf8 must not be null");
        }
        if (off < 0 || len < 0 || off + len > utf8.length) {
            throw new IndexOutOfBoundsException();
        }
        JsonLimits lim = JsonLimits.orDefault(limits);
        String s = strictUtf8ToString(utf8, off, len);
        return parse(s, lim);
    }

    public static JsonValue parseStrictUtf8(ByteBuffer utf8, JsonLimits limits) {
        if (utf8 == null) {
            throw new IllegalArgumentException("utf8 must not be null");
        }
        JsonLimits lim = JsonLimits.orDefault(limits);
        String s = strictUtf8ToString(utf8);
        return parse(s, lim);
    }

    public static JsonValue parse(String json, JsonLimits limits) {
        if (json == null) {
            throw new IllegalArgumentException("json must not be null");
        }
        JsonLimits lim = JsonLimits.orDefault(limits);
        Parser p = new Parser(json, lim);
        JsonValue v = p.parseValue(0);
        p.skipWs();
        if (!p.eof()) {
            throw p.error("Trailing data");
        }
        return v;
    }

    private static String strictUtf8ToString(byte[] utf8, int off, int len) {
        CharsetDecoder dec = TL_UTF8_DECODER.get();
        dec.reset();
        try {
            CharBuffer cb = dec.decode(ByteBuffer.wrap(utf8, off, len));
            return cb.toString();
        } catch (CharacterCodingException e) {
            throw new JsonParseException("Invalid UTF-8");
        }
    }

    private static String strictUtf8ToString(ByteBuffer utf8) {
        CharsetDecoder dec = TL_UTF8_DECODER.get();
        dec.reset();
        try {
            CharBuffer cb = dec.decode(utf8.duplicate());
            return cb.toString();
        } catch (CharacterCodingException e) {
            throw new JsonParseException("Invalid UTF-8");
        }
    }

    private static final class Parser {
        private final String s;
        private final JsonLimits limits;
        private int pos;

        private Parser(String s, JsonLimits limits) {
            this.s = s;
            this.limits = limits;
        }

        private boolean eof() {
            return pos >= s.length();
        }

        private char peek() {
            return s.charAt(pos);
        }

        private char next() {
            return s.charAt(pos++);
        }

        private void skipWs() {
            while (!eof()) {
                char c = peek();
                if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
                    pos++;
                    continue;
                }
                return;
            }
        }

        private JsonParseException error(String message) {
            int p = Math.max(0, Math.min(pos, s.length()));
            return new JsonParseException(message + " at pos " + p);
        }

        private JsonValue parseValue(int depth) {
            if (depth >= limits.maxNestingDepth()) {
                throw error("Max nesting depth exceeded");
            }
            skipWs();
            if (eof()) {
                throw error("Unexpected EOF");
            }
            char c = peek();
            if (c == '{') {
                return parseObject(depth + 1);
            }
            if (c == '[') {
                return parseArray(depth + 1);
            }
            if (c == '"') {
                return new JsonString(parseString());
            }
            if (c == 't') {
                expectLiteral("true");
                return new JsonBoolean(true);
            }
            if (c == 'f') {
                expectLiteral("false");
                return new JsonBoolean(false);
            }
            if (c == 'n') {
                expectLiteral("null");
                return JsonNull.INSTANCE;
            }
            if (c == '-' || (c >= '0' && c <= '9')) {
                return parseNumber();
            }
            throw error("Unexpected character '" + c + "'");
        }

        private void expectLiteral(String lit) {
            for (int i = 0; i < lit.length(); i++) {
                if (eof() || next() != lit.charAt(i)) {
                    throw error("Expected '" + lit + "'");
                }
            }
        }

        private JsonObject parseObject(int depth) {
            if (next() != '{') {
                throw error("Expected '{'");
            }
            skipWs();
            if (!eof() && peek() == '}') {
                pos++;
                return new JsonObject(Map.of());
            }

            LinkedHashMap<String, JsonValue> map = new LinkedHashMap<>();
            int pairs = 0;
            while (true) {
                skipWs();
                if (eof() || peek() != '"') {
                    throw error("Expected object key string");
                }
                String key = parseString();
                pairs++;
                if (limits.maxObjectPairs() > 0 && pairs > limits.maxObjectPairs()) {
                    throw error("Max object pairs exceeded");
                }
                skipWs();
                if (eof() || next() != ':') {
                    throw error("Expected ':'");
                }
                JsonValue value = parseValue(depth);
                map.put(key, value);
                skipWs();
                if (eof()) {
                    throw error("Unexpected EOF");
                }
                char c = next();
                if (c == '}') {
                    return new JsonObject(map);
                }
                if (c == ',') {
                    continue;
                }
                throw error("Expected ',' or '}'");
            }
        }

        private JsonArray parseArray(int depth) {
            if (next() != '[') {
                throw error("Expected '['");
            }
            skipWs();
            if (!eof() && peek() == ']') {
                pos++;
                return new JsonArray(List.of());
            }
            ArrayList<JsonValue> out = new ArrayList<>();
            while (true) {
                out.add(parseValue(depth));
                if (limits.maxArrayLen() > 0 && out.size() > limits.maxArrayLen()) {
                    throw error("Max array length exceeded");
                }
                skipWs();
                if (eof()) {
                    throw error("Unexpected EOF");
                }
                char c = next();
                if (c == ']') {
                    return new JsonArray(out);
                }
                if (c == ',') {
                    continue;
                }
                throw error("Expected ',' or ']'");
            }
        }

        private String parseString() {
            if (next() != '"') {
                throw error("Expected '\"'");
            }
            StringBuilder sb = new StringBuilder();
            while (!eof()) {
                char c = next();
                if (c == '"') {
                    if (limits.maxStringChars() > 0 && sb.length() > limits.maxStringChars()) {
                        throw error("Max string length exceeded");
                    }
                    return sb.toString();
                }
                if (c == '\\') {
                    if (eof()) {
                        throw error("Unexpected EOF in escape");
                    }
                    char esc = next();
                    switch (esc) {
                        case '"':
                        case '\\':
                        case '/':
                            sb.append(esc);
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'u':
                            sb.append(parseHex4());
                            break;
                        default:
                            throw error("Invalid escape '\\" + esc + "'");
                    }
                    continue;
                }
                if (c <= 0x1F) {
                    throw error("Unescaped control character in string");
                }
                sb.append(c);
                if (limits.maxStringChars() > 0 && sb.length() > limits.maxStringChars()) {
                    throw error("Max string length exceeded");
                }
            }
            throw error("Unterminated string");
        }

        private char parseHex4() {
            int v = 0;
            for (int i = 0; i < 4; i++) {
                if (eof()) {
                    throw error("Unexpected EOF in unicode escape");
                }
                char c = next();
                int d;
                if (c >= '0' && c <= '9') {
                    d = c - '0';
                } else if (c >= 'a' && c <= 'f') {
                    d = 10 + (c - 'a');
                } else if (c >= 'A' && c <= 'F') {
                    d = 10 + (c - 'A');
                } else {
                    throw error("Invalid hex digit in unicode escape");
                }
                v = (v << 4) | d;
            }
            return (char) v;
        }

        private JsonNumber parseNumber() {
            int start = pos;
            if (peek() == '-') {
                pos++;
                if (eof()) {
                    throw error("Invalid number");
                }
            }
            if (peek() == '0') {
                pos++;
            } else {
                if (eof() || peek() < '0' || peek() > '9') {
                    throw error("Invalid number");
                }
                while (!eof() && peek() >= '0' && peek() <= '9') {
                    pos++;
                }
            }
            boolean hasFrac = false;
            boolean hasExp = false;
            if (!eof() && peek() == '.') {
                hasFrac = true;
                pos++;
                if (eof() || peek() < '0' || peek() > '9') {
                    throw error("Invalid number");
                }
                while (!eof() && peek() >= '0' && peek() <= '9') {
                    pos++;
                }
            }
            if (!eof() && (peek() == 'e' || peek() == 'E')) {
                hasExp = true;
                pos++;
                if (!eof() && (peek() == '+' || peek() == '-')) {
                    pos++;
                }
                if (eof() || peek() < '0' || peek() > '9') {
                    throw error("Invalid number");
                }
                while (!eof() && peek() >= '0' && peek() <= '9') {
                    pos++;
                }
            }

            String raw = s.substring(start, pos);
            if (!hasFrac && !hasExp) {
                try {
                    long v = Long.parseLong(raw);
                    return new JsonLong(v);
                } catch (NumberFormatException e) {
                    throw error("Integer out of range");
                }
            }
            try {
                double dv = Double.parseDouble(raw);
                if (Double.isNaN(dv) || Double.isInfinite(dv)) {
                    throw error("Non-finite number");
                }
                return new JsonDouble(dv);
            } catch (NumberFormatException e) {
                throw error("Invalid number");
            }
        }
    }
}
