package yier.bubu.redis.protocol.custom.v1.wire;

import yier.bubu.redis.protocol.custom.v1.json.JsonBoolean;
import yier.bubu.redis.protocol.custom.v1.json.JsonArray;
import yier.bubu.redis.protocol.custom.v1.json.JsonLimits;
import yier.bubu.redis.protocol.custom.v1.json.JsonObject;
import yier.bubu.redis.protocol.custom.v1.json.JsonParser;
import yier.bubu.redis.protocol.custom.v1.json.JsonValue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Custom Protocol v1 reply parser and tagged-value helpers shared by client-side tools.
 */
public final class CustomProtocolV1ReplyParser {
    private CustomProtocolV1ReplyParser() {
    }

    public static ParsedReply parse(byte[] line) {
        Objects.requireNonNull(line, "line");
        return parse(line, 0, line.length);
    }

    public static ParsedReply parse(byte[] line, int off, int len) {
        Objects.requireNonNull(line, "line");
        if (off < 0 || len < 0 || off + len > line.length) {
            throw new IndexOutOfBoundsException();
        }
        JsonObject envelope = requireEnvelopeObject(JsonParser.parseStrictUtf8(line, off, len, JsonLimits.DEFAULT));
        byte[] rawLine = off == 0 && len == line.length ? line : Arrays.copyOfRange(line, off, off + len);
        return new ParsedReply(rawLine, envelope);
    }

    public static boolean isOkEnvelope(JsonValue envelope) {
        JsonObject obj = requireEnvelopeObject(envelope);
        JsonValue ok = obj.values().get("ok");
        return ok instanceof JsonBoolean b && b.value();
    }

    public static JsonObject envelopeObject(JsonValue envelope) {
        return detachObject(requireEnvelopeObject(envelope));
    }

    public static JsonValue resultValue(JsonValue envelope) {
        return detachValue(requireEnvelopeObject(envelope).values().get("result"));
    }

    public static JsonObject errorObject(JsonValue envelope) {
        JsonObject obj = requireEnvelopeObject(envelope);
        JsonValue error = obj.values().get("error");
        if (!(error instanceof JsonObject err)) {
            throw new IllegalArgumentException("expected error object");
        }
        return detachObject(err);
    }

    public static Map<String, JsonValue> decodeResultMapStringKeys(JsonValue envelope) {
        JsonValue result = requireEnvelopeObject(envelope).values().get("result");
        if (!(result instanceof JsonObject obj)) {
            throw new IllegalArgumentException("expected result to be a JSON object");
        }
        LinkedHashMap<String, JsonValue> out = new LinkedHashMap<>(Math.max(16, obj.values().size() * 2));
        if (CustomProtocolV1TaggedValue.isTaggedMap(obj)) {
            for (Map.Entry<String, JsonValue> entry : CustomProtocolV1TaggedValue.decodeTaggedMapToStringKeyedObject(obj).entrySet()) {
                out.put(entry.getKey(), detachValue(entry.getValue()));
            }
            return Collections.unmodifiableMap(out);
        }
        for (Map.Entry<String, JsonValue> entry : obj.values().entrySet()) {
            out.put(entry.getKey(), detachValue(entry.getValue()));
        }
        return Collections.unmodifiableMap(out);
    }

    public static byte[] decodeBytesOrNull(JsonValue value) {
        return CustomProtocolV1TaggedValue.decodeBytesOrNull(value);
    }

    public static String decodeUtf8StringOrNull(JsonValue value) {
        byte[] bytes = decodeBytesOrNull(value);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static JsonObject requireEnvelopeObject(JsonValue envelope) {
        if (!(envelope instanceof JsonObject obj)) {
            throw new IllegalArgumentException("expected reply envelope to be a JSON object");
        }
        return obj;
    }

    private static JsonObject detachObject(JsonObject value) {
        return (JsonObject) detachValue(value);
    }

    private static JsonValue detachValue(JsonValue value) {
        if (value == null) {
            return null;
        }
        if (value instanceof JsonObject obj) {
            LinkedHashMap<String, JsonValue> out = new LinkedHashMap<>(Math.max(16, obj.values().size() * 2));
            for (Map.Entry<String, JsonValue> entry : obj.values().entrySet()) {
                out.put(entry.getKey(), detachValue(entry.getValue()));
            }
            return new JsonObject(Collections.unmodifiableMap(out));
        }
        if (value instanceof JsonArray arr) {
            ArrayList<JsonValue> out = new ArrayList<>(arr.values().size());
            for (JsonValue entry : arr.values()) {
                out.add(detachValue(entry));
            }
            return new JsonArray(Collections.unmodifiableList(out));
        }
        return value;
    }

    public record ParsedReply(byte[] line, JsonObject envelope) {
        public ParsedReply {
            Objects.requireNonNull(line, "line");
            Objects.requireNonNull(envelope, "envelope");
            line = line.clone();
            envelope = detachObject(envelope);
        }

        @Override
        public byte[] line() {
            return line.clone();
        }

        public String lineUtf8() {
            return new String(line, StandardCharsets.UTF_8);
        }

        public boolean isOkEnvelope() {
            return CustomProtocolV1ReplyParser.isOkEnvelope(envelope);
        }

        public JsonValue resultValue() {
            return CustomProtocolV1ReplyParser.resultValue(envelope);
        }

        public JsonObject errorObject() {
            return CustomProtocolV1ReplyParser.errorObject(envelope);
        }

        public Map<String, JsonValue> decodeResultMapStringKeys() {
            return CustomProtocolV1ReplyParser.decodeResultMapStringKeys(envelope);
        }
    }
}
