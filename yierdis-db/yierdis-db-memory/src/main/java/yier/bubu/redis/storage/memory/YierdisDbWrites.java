package yier.bubu.redis.storage.memory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.HllWriteOps;
import yier.bubu.redis.storage.api.KeyspaceWriteOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.TtlWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.expire.YierdisTtlOps;

public final class YierdisDbWrites implements DbWrites {
    private final YierdisDbRuntimeInternals internals;
    private final YierdisStringOps strings;
    private final YierdisHashOps hashes;
    private final YierdisListOps lists;
    private final YierdisSetOps sets;
    private final YierdisZSetOps zsets;
    private final YierdisHllOps hll;
    private final YierdisKeyspaceOps keyspace;
    private final YierdisTtlOps ttl;

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
        return new ContextualWrites(Objects.requireNonNull(context, "context"));
    }

    private final class ContextualWrites implements
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

        private ContextualWrites(MutationContext context) {
            this.context = context;
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
            return new ContextualWrites(Objects.requireNonNull(context, "context"));
        }

        @Override
        public WriteResult<StringWriteOps.SetStringValue> set(
                byte[] keyBytes,
                BytesSlice value,
                SetMode mode,
                ExpireOption expireOption
        ) {
            return strings.set(context, keyBytes, value, mode, expireOption);
        }

        @Override
        public PreparedMutation<StringWriteOps.SetStringValue> prepareSet(
                byte[] keyBytes,
                BytesSlice value,
                SetMode mode,
                ExpireOption expireOption,
                boolean returnOldValue
        ) {
            return strings.prepareSet(keyBytes, value, mode, expireOption, returnOldValue);
        }

        @Override
        public WriteResult<Boolean> setString(
                byte[] keyBytes,
                byte[] value,
                SetMode mode,
                ExpireOption expireOption
        ) {
            return strings.setString(context, keyBytes, value, mode, expireOption);
        }

        @Override
        public WriteResult<Boolean> setString(
                byte[] keyBytes,
                BytesSlice value,
                SetMode mode,
                ExpireOption expireOption
        ) {
            return strings.setString(context, keyBytes, value, mode, expireOption);
        }

        @Override
        public WriteResult<Long> append(byte[] keyBytes, BytesSlice value) {
            return strings.append(context, keyBytes, value);
        }

        @Override
        public WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value) {
            return strings.setBit(context, keyBytes, offset, value);
        }

        @Override
        public WriteResult<Long> incrBy(byte[] keyBytes, long delta) {
            return strings.incrBy(context, keyBytes, delta);
        }

        @Override
        public WriteResult<Long> hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
            return hashes.hset(context, keyBytes, fieldValuePairs);
        }

        @Override
        public WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields) {
            return hashes.hdel(context, keyBytes, fields);
        }

        @Override
        public WriteResult<Long> lpush(byte[] keyBytes, List<byte[]> values) {
            return lists.lpush(context, keyBytes, values);
        }

        @Override
        public WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values) {
            return lists.rpush(context, keyBytes, values);
        }

        @Override
        public PreparedMutation<PoppedValueSequence> preparePop(byte[] keyBytes, int count, boolean left) {
            return lists.preparePop(keyBytes, count, left);
        }

        @Override
        public WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members) {
            return sets.sadd(context, keyBytes, members);
        }

        @Override
        public WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members) {
            return sets.srem(context, keyBytes, members);
        }

        @Override
        public WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
            return zsets.zadd(context, keyBytes, scoreMemberPairs);
        }

        @Override
        public WriteResult<Long> zremrangeByScore(
                byte[] keyBytes,
                double min,
                boolean minExclusive,
                double max,
                boolean maxExclusive
        ) {
            return zsets.zremrangeByScore(context, keyBytes, min, minExclusive, max, maxExclusive);
        }

        @Override
        public WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop) {
            return zsets.zremrangeByRank(context, keyBytes, start, stop);
        }

        @Override
        public WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members) {
            return zsets.zrem(context, keyBytes, members);
        }

        @Override
        public WriteResult<Integer> pfadd(byte[] keyBytes, List<byte[]> elements) {
            return hll.pfadd(context, keyBytes, elements);
        }

        @Override
        public WriteResult<Void> pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys) {
            return hll.pfmerge(context, destKeyBytes, sourceKeys);
        }

        @Override
        public WriteResult<Long> del(Collection<byte[]> keys) {
            return keyspace.del(context, keys);
        }

        @Override
        public WriteResult<Boolean> expire(BytesView keyView, long seconds) {
            return ttl.expire(context, keyView, seconds);
        }

        @Override
        public WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds) {
            return ttl.pexpire(context, keyView, milliseconds);
        }

        @Override
        public WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds) {
            return ttl.expireAtSeconds(context, keyView, unixSeconds);
        }

        @Override
        public WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis) {
            return ttl.expireAtMillis(context, keyView, unixMillis);
        }

        @Override
        public WriteResult<Boolean> persist(BytesView keyView) {
            return ttl.persist(context, keyView);
        }
    }
}
