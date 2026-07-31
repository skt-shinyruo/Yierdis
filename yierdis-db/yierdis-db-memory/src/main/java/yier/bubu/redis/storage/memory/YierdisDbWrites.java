package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.HllWriteOps;
import yier.bubu.redis.storage.api.KeyspaceWriteOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.TtlWriteOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.memory.internal.expire.YierdisTtlOps;

public final class YierdisDbWrites implements DbWrites {
    private final YierdisDbRuntimeInternals internals;
    private final StringWriteOps strings;
    private final HashWriteOps hashes;
    private final ListWriteOps lists;
    private final SetWriteOps sets;
    private final ZSetWriteOps zsets;
    private final HllWriteOps hll;
    private final KeyspaceWriteOps keyspace;
    private final TtlWriteOps ttl;

    YierdisDbWrites(
            YierdisDbRuntimeInternals internals,
            YierdisStringOps strings,
            YierdisHashOps hashes,
            YierdisListOps lists,
            YierdisSetOps sets,
            YierdisZSetOps zsets,
            YierdisHllOps hll,
            YierdisKeyspaceOps keyspace,
            YierdisTtlOps ttl
    ) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.strings = Objects.requireNonNull(strings, "strings");
        this.hashes = Objects.requireNonNull(hashes, "hashes");
        this.lists = Objects.requireNonNull(lists, "lists");
        this.sets = Objects.requireNonNull(sets, "sets");
        this.zsets = Objects.requireNonNull(zsets, "zsets");
        this.hll = Objects.requireNonNull(hll, "hll");
        this.keyspace = Objects.requireNonNull(keyspace, "keyspace");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
    }

    private YierdisDbWrites(YierdisDbRuntimeInternals internals) {
        this(
                internals,
                new YierdisStringOps(internals),
                new YierdisHashOps(internals),
                new YierdisListOps(internals),
                new YierdisSetOps(internals),
                new YierdisZSetOps(internals),
                new YierdisHllOps(internals),
                new YierdisKeyspaceOps(internals),
                new YierdisTtlOps(internals)
        );
    }

    @Override
    public StringWriteOps strings() {
        return strings;
    }

    @Override
    public HashWriteOps hashes() {
        return hashes;
    }

    @Override
    public ListWriteOps lists() {
        return lists;
    }

    @Override
    public SetWriteOps sets() {
        return sets;
    }

    @Override
    public ZSetWriteOps zsets() {
        return zsets;
    }

    @Override
    public HllWriteOps hll() {
        return hll;
    }

    @Override
    public KeyspaceWriteOps keyspace() {
        return keyspace;
    }

    @Override
    public TtlWriteOps ttl() {
        return ttl;
    }

    @Override
    public DbWrites withMutationContext(MutationContext context) {
        return new YierdisDbWrites(internals.withMutationContext(context));
    }
}
