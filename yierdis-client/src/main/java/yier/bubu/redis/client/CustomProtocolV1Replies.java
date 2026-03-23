package yier.bubu.redis.client;

// Custom Protocol v1 reply 解码辅助：提供对 $map/$b64/$error 的最小解析能力（面向 client/CLI/bench）。

import yier.bubu.redis.protocol.json.JsonBoolean;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonValue;
import yier.bubu.redis.protocol.v1.CustomProtocolV1TaggedValue;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class CustomProtocolV1Replies {
    private CustomProtocolV1Replies() {
    }

    public static boolean isOkEnvelope(JsonValue envelope) {
        JsonObject obj = envelopeObject(envelope);
        JsonValue ok = obj.values().get("ok");
        return ok instanceof JsonBoolean b && b.value();
    }

    public static JsonObject envelopeObject(JsonValue envelope) {
        if (!(envelope instanceof JsonObject obj)) {
            throw new IllegalArgumentException("expected reply envelope to be a JSON object");
        }
        return obj;
    }

    public static JsonValue resultValue(JsonValue envelope) {
        JsonObject obj = envelopeObject(envelope);
        return obj.values().get("result");
    }

    public static JsonObject errorObject(JsonValue envelope) {
        JsonObject obj = envelopeObject(envelope);
        JsonValue e = obj.values().get("error");
        if (!(e instanceof JsonObject err)) {
            throw new IllegalArgumentException("expected error object");
        }
        return err;
    }

    public static Map<String, JsonValue> decodeResultMapStringKeys(JsonValue envelope) {
        JsonValue result = resultValue(envelope);
        if (!(result instanceof JsonObject obj)) {
            throw new IllegalArgumentException("expected result to be a JSON object");
        }
        if (CustomProtocolV1TaggedValue.isTaggedMap(obj)) {
            return CustomProtocolV1TaggedValue.decodeTaggedMapToStringKeyedObject(obj);
        }
        return obj.values();
    }

    public static byte[] decodeBytesOrNull(JsonValue v) {
        return CustomProtocolV1TaggedValue.decodeBytesOrNull(v);
    }

    public static String decodeUtf8StringOrNull(JsonValue v) {
        byte[] bytes = decodeBytesOrNull(v);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
