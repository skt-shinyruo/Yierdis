package yier.bubu.redis.protocol.json;

import java.util.List;
import java.util.Objects;

public record JsonArray(List<JsonValue> values) implements JsonValue {
    public JsonArray {
        values = Objects.requireNonNull(values, "values");
    }
}
