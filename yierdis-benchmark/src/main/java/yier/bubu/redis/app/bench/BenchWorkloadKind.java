package yier.bubu.redis.app.bench;

public enum BenchWorkloadKind {
    PING,
    SET_GET,
    APPEND,
    HLL_SPARSE,
    HLL_DENSE,
    HLL_PFCOUNT,
    NATIVE_DEFRAG_APPEND,
    MAXMEMORY_EVICTION,
    TTL_EXPIRATION,
    LIST_LPUSH,
    HASH_HSET,
    SET_SADD,
    ZSET_ZADD,
    SCAN,
    MIXED_READ_WRITE
}
