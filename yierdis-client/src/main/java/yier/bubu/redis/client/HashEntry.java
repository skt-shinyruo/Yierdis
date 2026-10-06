package yier.bubu.redis.client;

/** 哈希字段和值，顺序与回复里的那一对一致。 */
public record HashEntry(String field, String value) {
}
