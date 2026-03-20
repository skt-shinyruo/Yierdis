package yier.bubu.redis.ops;

// DbWrites：对 command-facing DB 能力做写路径分组，逐步替代 legacy mixed ops 视图。

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
