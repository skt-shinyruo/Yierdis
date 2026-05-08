package yier.bubu.redis.protocol.custom.v1.json;

public final class JsonNull implements JsonValue {
    public static final JsonNull INSTANCE = new JsonNull();

    private JsonNull() {
    }

    @Override
    public String toString() {
        return "null";
    }
}
