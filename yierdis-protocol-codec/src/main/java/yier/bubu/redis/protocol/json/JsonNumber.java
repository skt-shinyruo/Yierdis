package yier.bubu.redis.protocol.json;

public sealed interface JsonNumber extends JsonValue permits JsonLong, JsonDouble {
}
