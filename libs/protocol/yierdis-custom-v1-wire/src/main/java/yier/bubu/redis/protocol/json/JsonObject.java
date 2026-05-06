package yier.bubu.redis.protocol.json;

import java.util.Map;
import java.util.Objects;

public record JsonObject(Map<String, JsonValue> values) implements JsonValue {
    public JsonObject {
        values = Objects.requireNonNull(values, "values");
    }
}
