package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.HashReadOps;
import yier.bubu.redis.storage.api.HllReadOps;
import yier.bubu.redis.storage.api.KeyspaceReadOps;
import yier.bubu.redis.storage.api.ListReadOps;
import yier.bubu.redis.storage.api.SetReadOps;
import yier.bubu.redis.storage.api.StringReadOps;
import yier.bubu.redis.storage.api.TtlReadOps;
import yier.bubu.redis.storage.api.ZSetReadOps;

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
