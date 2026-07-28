package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.HllWriteOps;
import yier.bubu.redis.storage.api.KeyspaceWriteOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.TtlWriteOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.command.MutationContext;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public final class YierdisDbWrites implements DbWrites {
    private final YierdisDbInternals internals;
    private final StringWriteOps strings;
    private final HashWriteOps hashes;
    private final ListWriteOps lists;
    private final SetWriteOps sets;
    private final ZSetWriteOps zsets;
    private final HllWriteOps hll;
    private final KeyspaceWriteOps keyspace;
    private final TtlWriteOps ttl;

    YierdisDbWrites(
            YierdisDbInternals internals,
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
        internals.checkThread();
        return new ContextualDbWrites(Objects.requireNonNull(context, "context"));
    }

    private final class ContextualDbWrites implements
            DbWrites,
            StringWriteOps,
            HashWriteOps,
            ListWriteOps,
            SetWriteOps,
            ZSetWriteOps,
            HllWriteOps,
            KeyspaceWriteOps,
            TtlWriteOps {
        private final MutationContext context;

        private ContextualDbWrites(MutationContext context) {
            this.context = context;
        }

        private <T> T invoke(java.util.function.Supplier<T> action) {
            return internals.withMutationContext(context, action);
        }

        @Override
        public StringWriteOps strings() {
            return this;
        }

        @Override
        public HashWriteOps hashes() {
            return this;
        }

        @Override
        public ListWriteOps lists() {
            return this;
        }

        @Override
        public SetWriteOps sets() {
            return this;
        }

        @Override
        public ZSetWriteOps zsets() {
            return this;
        }

        @Override
        public HllWriteOps hll() {
            return this;
        }

        @Override
        public KeyspaceWriteOps keyspace() {
            return this;
        }

        @Override
        public TtlWriteOps ttl() {
            return this;
        }

        @Override
        public DbWrites withMutationContext(MutationContext context) {
            internals.checkThread();
            return new ContextualDbWrites(Objects.requireNonNull(context, "context"));
        }

        @Override
        public WriteResult<SetStringValue> set(
                byte[] keyBytes,
                BytesSlice value,
                SetMode mode,
                ExpireOption expireOption
        ) {
            return invoke(() -> strings.set(keyBytes, value, mode, expireOption));
        }

        @Override
        public PreparedMutation<SetStringValue> prepareSet(
                byte[] keyBytes,
                BytesSlice value,
                SetMode mode,
                ExpireOption expireOption,
                boolean returnOldValue
        ) {
            return invoke(() -> strings.prepareSet(keyBytes, value, mode, expireOption, returnOldValue));
        }

        @Override
        public WriteResult<Boolean> setString(
                byte[] keyBytes,
                byte[] value,
                SetMode mode,
                ExpireOption expireOption
        ) {
            return invoke(() -> strings.setString(keyBytes, value, mode, expireOption));
        }

        @Override
        public WriteResult<Boolean> setString(
                byte[] keyBytes,
                BytesSlice value,
                SetMode mode,
                ExpireOption expireOption
        ) {
            return invoke(() -> strings.setString(keyBytes, value, mode, expireOption));
        }

        @Override
        public WriteResult<Long> append(byte[] keyBytes, BytesSlice value) {
            return invoke(() -> strings.append(keyBytes, value));
        }

        @Override
        public WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value) {
            return invoke(() -> strings.setBit(keyBytes, offset, value));
        }

        @Override
        public WriteResult<Long> incrBy(byte[] keyBytes, long delta) {
            return invoke(() -> strings.incrBy(keyBytes, delta));
        }

        @Override
        public WriteResult<Long> hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
            return invoke(() -> hashes.hset(keyBytes, fieldValuePairs));
        }

        @Override
        public WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields) {
            return invoke(() -> hashes.hdel(keyBytes, fields));
        }

        @Override
        public WriteResult<Long> lpush(byte[] keyBytes, List<byte[]> values) {
            return invoke(() -> lists.lpush(keyBytes, values));
        }

        @Override
        public WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values) {
            return invoke(() -> lists.rpush(keyBytes, values));
        }

        @Override
        public PreparedMutation<PoppedValueSequence> preparePop(byte[] keyBytes, int count, boolean left) {
            return invoke(() -> lists.preparePop(keyBytes, count, left));
        }

        @Override
        public WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members) {
            return invoke(() -> sets.sadd(keyBytes, members));
        }

        @Override
        public WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members) {
            return invoke(() -> sets.srem(keyBytes, members));
        }

        @Override
        public WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
            return invoke(() -> zsets.zadd(keyBytes, scoreMemberPairs));
        }

        @Override
        public WriteResult<Long> zremrangeByScore(
                byte[] keyBytes,
                double min,
                boolean minExclusive,
                double max,
                boolean maxExclusive
        ) {
            return invoke(() -> zsets.zremrangeByScore(
                    keyBytes,
                    min,
                    minExclusive,
                    max,
                    maxExclusive
            ));
        }

        @Override
        public WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop) {
            return invoke(() -> zsets.zremrangeByRank(keyBytes, start, stop));
        }

        @Override
        public WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members) {
            return invoke(() -> zsets.zrem(keyBytes, members));
        }

        @Override
        public WriteResult<Integer> pfadd(byte[] keyBytes, List<byte[]> elements) {
            return invoke(() -> hll.pfadd(keyBytes, elements));
        }

        @Override
        public WriteResult<Void> pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys) {
            return invoke(() -> hll.pfmerge(destKeyBytes, sourceKeys));
        }

        @Override
        public WriteResult<Long> del(Collection<byte[]> keys) {
            return invoke(() -> keyspace.del(keys));
        }

        @Override
        public WriteResult<Boolean> expire(BytesView keyView, long seconds) {
            return invoke(() -> ttl.expire(keyView, seconds));
        }

        @Override
        public WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds) {
            return invoke(() -> ttl.pexpire(keyView, milliseconds));
        }

        @Override
        public WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds) {
            return invoke(() -> ttl.expireAtSeconds(keyView, unixSeconds));
        }

        @Override
        public WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis) {
            return invoke(() -> ttl.expireAtMillis(keyView, unixMillis));
        }

        @Override
        public WriteResult<Boolean> persist(BytesView keyView) {
            return invoke(() -> ttl.persist(keyView));
        }
    }
}
