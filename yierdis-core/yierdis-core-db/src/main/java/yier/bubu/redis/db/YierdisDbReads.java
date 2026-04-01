package yier.bubu.redis.db;

import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.HashReadOps;
import yier.bubu.redis.ops.HllReadOps;
import yier.bubu.redis.ops.KeyspaceReadOps;
import yier.bubu.redis.ops.ListReadOps;
import yier.bubu.redis.ops.SetReadOps;
import yier.bubu.redis.ops.StringReadOps;
import yier.bubu.redis.ops.TtlReadOps;
import yier.bubu.redis.ops.ZSetReadOps;

import java.util.Objects;

final class YierdisDbReads implements DbReads {
    private final StringReadOps strings;
    private final HashReadOps hashes;
    private final ListReadOps lists;
    private final SetReadOps sets;
    private final ZSetReadOps zsets;
    private final HllReadOps hll;
    private final KeyspaceReadOps keyspace;
    private final TtlReadOps ttl;

    YierdisDbReads(
            YierdisStringOps strings,
            YierdisHashOps hashes,
            YierdisListOps lists,
            YierdisSetOps sets,
            YierdisZSetOps zsets,
            YierdisHllOps hll,
            YierdisKeyspaceOps keyspace,
            YierdisTtlOps ttl
    ) {
        this.strings = Objects.requireNonNull(strings, "strings");
        this.hashes = Objects.requireNonNull(hashes, "hashes");
        this.lists = Objects.requireNonNull(lists, "lists");
        this.sets = Objects.requireNonNull(sets, "sets");
        this.zsets = Objects.requireNonNull(zsets, "zsets");
        this.hll = Objects.requireNonNull(hll, "hll");
        this.keyspace = Objects.requireNonNull(keyspace, "keyspace");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
    }

    @Override
    public StringReadOps strings() {
        return strings;
    }

    @Override
    public HashReadOps hashes() {
        return hashes;
    }

    @Override
    public ListReadOps lists() {
        return lists;
    }

    @Override
    public SetReadOps sets() {
        return sets;
    }

    @Override
    public ZSetReadOps zsets() {
        return zsets;
    }

    @Override
    public HllReadOps hll() {
        return hll;
    }

    @Override
    public KeyspaceReadOps keyspace() {
        return keyspace;
    }

    @Override
    public TtlReadOps ttl() {
        return ttl;
    }
}
