package yier.bubu.redis.protocol.custom.v1.json;

public sealed interface JsonNumber extends JsonValue permits JsonLong, JsonDouble {
}
