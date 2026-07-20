package yier.bubu.redis.storage.api;

// DbWrites groups command-facing DB write capabilities.

import java.util.Objects;
import yier.bubu.redis.common.command.MutationContext;

public interface DbWrites {
    StringWriteOps strings();

    HashWriteOps hashes();

    ListWriteOps lists();

    SetWriteOps sets();

    ZSetWriteOps zsets();

    HllWriteOps hll();

    KeyspaceWriteOps keyspace();

    TtlWriteOps ttl();

    default DbWrites withMutationContext(MutationContext context) {
        Objects.requireNonNull(context, "context");
        return this;
    }
}
