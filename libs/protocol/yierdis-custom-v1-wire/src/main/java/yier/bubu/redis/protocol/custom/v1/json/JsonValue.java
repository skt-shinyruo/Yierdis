package yier.bubu.redis.protocol.custom.v1.json;

public sealed interface JsonValue permits JsonNull, JsonBoolean, JsonString, JsonNumber, JsonArray, JsonObject {
}
