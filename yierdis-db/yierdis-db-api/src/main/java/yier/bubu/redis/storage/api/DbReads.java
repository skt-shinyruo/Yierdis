package yier.bubu.redis.storage.api;

// DbReads groups command-facing DB read capabilities.

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
