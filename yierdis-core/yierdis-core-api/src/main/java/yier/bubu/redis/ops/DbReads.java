package yier.bubu.redis.ops;

// DbReads：对 command-facing DB 能力做只读分组，逐步替代 legacy mixed ops 视图。

public interface DbReads {
    StringReadOps strings();

    HashReadOps hashes();

    ListReadOps lists();

    SetReadOps sets();

    ZSetReadOps zsets();

    HllReadOps hll();

    KeyspaceReadOps keyspace();

    TtlReadOps ttl();
}
