package yier.bubu.redis.storage.api;

import yier.bubu.redis.bytes.BytesView;

/**
 * DB 的语义操作边界，按数据类型提供操作、内存诊断、数据库管理与健康状态视图。
 */
public interface DbEngine {
    StringOps strings();

    HashOps hashes();

    ListOps lists();

    SetOps sets();

    ZSetOps zsets();

    HllOps hll();

    KeyspaceOps keyspace();

    TtlOps ttl();

    long memoryUsage(BytesView keyView);

    YierdisMemoryStats memoryStats();

    String objectEncoding(BytesView keyView);

    MutationOutcome flushDb();

    default MutationOutcome flushDbAsync() {
        return flushDb();
    }

    default DbHealthSnapshot health() {
        return DbHealthSnapshot.healthy();
    }
}
