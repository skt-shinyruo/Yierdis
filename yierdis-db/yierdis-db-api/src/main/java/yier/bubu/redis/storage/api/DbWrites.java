package yier.bubu.redis.storage.api;

// DbWrites groups command-facing DB write capabilities.

public interface DbWrites {
    StringWriteOps strings();

    HashWriteOps hashes();

    ListWriteOps lists();

    SetWriteOps sets();

    ZSetWriteOps zsets();

    HllWriteOps hll();

    KeyspaceWriteOps keyspace();

    TtlWriteOps ttl();
}
