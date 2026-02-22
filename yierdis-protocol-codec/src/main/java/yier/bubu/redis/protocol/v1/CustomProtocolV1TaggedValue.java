package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.protocol.json.JsonArray;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Custom Protocol v1 tagged value 的解码辅助（SSOT）。
 * <p>
 * 约定：
 * - map：{@code {"$map":[[k,v],...]}}
 * - bytes（非 UTF-8）：{@code {"$b64":"..."}}
 * - nested error：{@code {"$error":{"kind":"...","message":"..."}}}
 */
public final class CustomProtocolV1TaggedValue {
    private CustomProtocolV1TaggedValue() {
    }

    public record MapEntry(JsonValue key, JsonValue value) {
        public MapEntry {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }
    }

    public static boolean isTaggedMap(JsonValue v) {
        if (!(v instanceof JsonObject obj)) {
            return false;
        }
        JsonValue map = obj.values().get(CustomProtocolV1NdjsonEncoder.TAG_MAP);
        return map instanceof JsonArray;
    }

    public static List<MapEntry> decodeTaggedMapEntries(JsonValue v) {
        if (!(v instanceof JsonObject obj)) {
            throw new IllegalArgumentException("expected tagged map object");
        }
        JsonValue map = obj.values().get(CustomProtocolV1NdjsonEncoder.TAG_MAP);
        if (!(map instanceof JsonArray arr)) {
            throw new IllegalArgumentException("expected $map array");
        }
        List<JsonValue> values = arr.values();
        if (values == null || values.isEmpty()) {
            return List.of();
        }

        ArrayList<MapEntry> out = new ArrayList<>(values.size());
        for (JsonValue e : values) {
            if (!(e instanceof JsonArray pair)) {
                throw new IllegalArgumentException("expected $map element to be an array pair");
            }
            List<JsonValue> pairVals = pair.values();
            if (pairVals == null || pairVals.size() != 2) {
                throw new IllegalArgumentException("expected $map pair size=2");
            }
            JsonValue k = pairVals.get(0);
            JsonValue val = pairVals.get(1);
            out.add(new MapEntry(k, val));
        }
        return out;
    }

    public static Map<String, JsonValue> decodeTaggedMapToStringKeyedObject(JsonValue v) {
        List<MapEntry> entries = decodeTaggedMapEntries(v);
        LinkedHashMap<String, JsonValue> out = new LinkedHashMap<>(Math.max(16, entries.size() * 2));
        for (MapEntry e : entries) {
            if (!(e.key() instanceof JsonString s)) {
                throw new IllegalArgumentException("expected $map key to be a string");
            }
            out.put(s.value(), e.value());
        }
        return out;
    }

    public static byte[] decodeBytesOrNull(JsonValue v) {
        if (v == null) {
            return null;
        }
        if (v instanceof JsonString s) {
            return s.value() == null ? null : s.value().getBytes(StandardCharsets.UTF_8);
        }
        if (v instanceof JsonObject obj) {
            JsonValue b64 = obj.values().get(CustomProtocolV1NdjsonEncoder.TAG_B64);
            if (b64 instanceof JsonString bs) {
                if (bs.value() == null) {
                    return null;
                }
                return Base64.getDecoder().decode(bs.value());
            }
        }
        return null;
    }
}
