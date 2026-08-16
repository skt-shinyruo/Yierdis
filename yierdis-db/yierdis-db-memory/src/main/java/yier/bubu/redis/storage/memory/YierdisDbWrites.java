package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.HllWriteOps;
import yier.bubu.redis.storage.api.KeyspaceWriteOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.TtlWriteOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;

final class YierdisDbWrites implements DbWrites {
    private final YierdisStringOps strings;
    private final YierdisHashOps hashes;
    private final YierdisListOps lists;
    private final YierdisSetOps sets;
    private final YierdisZSetOps zsets;
    private final YierdisHllOps hll;
    private final YierdisKeyspaceOps keyspace;
    private final YierdisTtlOps ttl;

    YierdisDbWrites(
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
}
