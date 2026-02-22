package yier.bubu.redis.protocol.json;

public sealed interface JsonValue permits JsonNull, JsonBoolean, JsonString, JsonNumber, JsonArray, JsonObject {
}
