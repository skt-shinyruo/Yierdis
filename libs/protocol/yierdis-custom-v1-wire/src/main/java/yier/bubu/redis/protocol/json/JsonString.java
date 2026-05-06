package yier.bubu.redis.protocol.json;

import java.util.Objects;

public record JsonString(String value) implements JsonValue {
    public JsonString {
        value = Objects.requireNonNull(value, "value");
    }
}
