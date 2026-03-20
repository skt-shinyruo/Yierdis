package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.HashOps;
import yier.bubu.redis.ops.HashWriteOps;
import yier.bubu.redis.ops.HllOps;
import yier.bubu.redis.ops.HllWriteOps;
import yier.bubu.redis.ops.KeyspaceOps;
import yier.bubu.redis.ops.KeyspaceWriteOps;
import yier.bubu.redis.ops.ListOps;
import yier.bubu.redis.ops.ListWriteOps;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.SetOps;
import yier.bubu.redis.ops.SetWriteOps;
import yier.bubu.redis.ops.StringOps;
import yier.bubu.redis.ops.StringWriteOps;
import yier.bubu.redis.ops.TtlOps;
import yier.bubu.redis.ops.TtlWriteOps;
import yier.bubu.redis.ops.ValueOps;
import yier.bubu.redis.ops.ZSetOps;
import yier.bubu.redis.ops.ZSetWriteOps;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

final class YierdisDbWrites implements DbWrites {
    private final YierdisDb db;
    private final StringWriteOps strings;
    private final HashWriteOps hashes;
    private final ListWriteOps lists;
    private final SetWriteOps sets;
    private final ZSetWriteOps zsets;
    private final HllWriteOps hll;
    private final KeyspaceWriteOps keyspace;
    private final TtlWriteOps ttl;

    YierdisDbWrites(YierdisDb db, ValueOps values, KeyspaceOps keyspaceOps, TtlOps ttlOps) {
        this.db = Objects.requireNonNull(db, "db");
        Objects.requireNonNull(values, "values");
        this.strings = new StringWrites(this.db);
        this.hashes = new HashWrites(values.hashes());
        this.lists = new ListWrites(values.lists());
        this.sets = new SetWrites(values.sets());
        this.zsets = new ZSetWrites(values.zsets());
        this.hll = new HllWrites(values.hll());
        this.keyspace = new KeyspaceWrites(this.db);
        this.ttl = new TtlWrites(this.db);
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

    private static final class StringWrites implements StringWriteOps {
        private final YierdisDb db;

        private StringWrites(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
            return db.setString(keyBytes, value, mode, expireOption);
        }

        @Override
        public boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
            return db.setString(keyBytes, value, mode, expireOption);
        }

        @Override
        public long append(byte[] keyBytes, BytesSlice value) {
            return db.append(keyBytes, value);
        }

        @Override
        public int setBit(byte[] keyBytes, long offset, int value) {
            return db.setBit(keyBytes, offset, value);
        }

        @Override
        public long incrBy(byte[] keyBytes, long delta) {
            return db.incrBy(keyBytes, delta);
        }
    }

    private static final class HashWrites implements HashWriteOps {
        private final HashOps delegate;

        private HashWrites(HashOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public long hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
            return delegate.hset(keyBytes, fieldValuePairs);
        }

        @Override
        public long hdel(byte[] keyBytes, List<byte[]> fields) {
            return delegate.hdel(keyBytes, fields);
        }
    }

    private static final class ListWrites implements ListWriteOps {
        private final ListOps delegate;

        private ListWrites(ListOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public long lpush(byte[] keyBytes, List<byte[]> values) {
            return delegate.lpush(keyBytes, values);
        }

        @Override
        public long rpush(byte[] keyBytes, List<byte[]> values) {
            return delegate.rpush(keyBytes, values);
        }

        @Override
        public List<byte[]> lpop(byte[] keyBytes, int count) {
            return delegate.lpop(keyBytes, count);
        }

        @Override
        public List<byte[]> rpop(byte[] keyBytes, int count) {
            return delegate.rpop(keyBytes, count);
        }
    }

    private static final class SetWrites implements SetWriteOps {
        private final SetOps delegate;

        private SetWrites(SetOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public long sadd(byte[] keyBytes, List<byte[]> members) {
            return delegate.sadd(keyBytes, members);
        }

        @Override
        public long srem(byte[] keyBytes, List<byte[]> members) {
            return delegate.srem(keyBytes, members);
        }
    }

    private static final class ZSetWrites implements ZSetWriteOps {
        private final ZSetOps delegate;

        private ZSetWrites(ZSetOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public long zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
            return delegate.zadd(keyBytes, scoreMemberPairs);
        }

        @Override
        public long zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
            return delegate.zremrangeByScore(keyBytes, min, minExclusive, max, maxExclusive);
        }

        @Override
        public long zremrangeByRank(byte[] keyBytes, long start, long stop) {
            return delegate.zremrangeByRank(keyBytes, start, stop);
        }

        @Override
        public long zrem(byte[] keyBytes, List<byte[]> members) {
            return delegate.zrem(keyBytes, members);
        }
    }

    private static final class HllWrites implements HllWriteOps {
        private final HllOps delegate;

        private HllWrites(HllOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public int pfadd(byte[] keyBytes, List<byte[]> elements) {
            return delegate.pfadd(keyBytes, elements);
        }

        @Override
        public void pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys) {
            delegate.pfmerge(destKeyBytes, sourceKeys);
        }
    }

    private static final class KeyspaceWrites implements KeyspaceWriteOps {
        private final YierdisDb db;

        private KeyspaceWrites(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long del(Collection<byte[]> keys) {
            return db.del(keys);
        }
    }

    private static final class TtlWrites implements TtlWriteOps {
        private final YierdisDb db;

        private TtlWrites(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public boolean expire(BytesView keyView, long seconds) {
            return db.expire(keyView, seconds);
        }

        @Override
        public boolean pexpire(BytesView keyView, long milliseconds) {
            return db.pexpire(keyView, milliseconds);
        }

        @Override
        public boolean expireAtSeconds(BytesView keyView, long unixSeconds) {
            return db.expireAtSeconds(keyView, unixSeconds);
        }

        @Override
        public boolean expireAtMillis(BytesView keyView, long unixMillis) {
            return db.expireAtMillis(keyView, unixMillis);
        }

        @Override
        public boolean persist(BytesView keyView) {
            return db.persist(keyView);
        }
    }
}
