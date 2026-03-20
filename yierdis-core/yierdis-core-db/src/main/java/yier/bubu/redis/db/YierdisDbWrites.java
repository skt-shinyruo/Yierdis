package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.HashWriteOps;
import yier.bubu.redis.ops.HllWriteOps;
import yier.bubu.redis.ops.KeyspaceWriteOps;
import yier.bubu.redis.ops.ListWriteOps;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.SetWriteOps;
import yier.bubu.redis.ops.StringWriteOps;
import yier.bubu.redis.ops.TtlWriteOps;
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

    YierdisDbWrites(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
        this.strings = new StringWrites(this.db);
        this.hashes = new HashWrites(this.db);
        this.lists = new ListWrites(this.db);
        this.sets = new SetWrites(this.db);
        this.zsets = new ZSetWrites(this.db);
        this.hll = new HllWrites(this.db);
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
        public SetStringResult set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption, boolean returnOldValue) {
            return db.setStringWithResult(keyBytes, value, mode, expireOption, returnOldValue);
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
        private final YierdisDb db;

        private HashWrites(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
            return db.hset(keyBytes, fieldValuePairs);
        }

        @Override
        public long hdel(byte[] keyBytes, List<byte[]> fields) {
            return db.hdel(keyBytes, fields);
        }
    }

    private static final class ListWrites implements ListWriteOps {
        private final YierdisDb db;

        private ListWrites(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long lpush(byte[] keyBytes, List<byte[]> values) {
            return db.lpush(keyBytes, values);
        }

        @Override
        public long rpush(byte[] keyBytes, List<byte[]> values) {
            return db.rpush(keyBytes, values);
        }

        @Override
        public List<byte[]> lpop(byte[] keyBytes, int count) {
            return db.lpop(keyBytes, count);
        }

        @Override
        public List<byte[]> rpop(byte[] keyBytes, int count) {
            return db.rpop(keyBytes, count);
        }
    }

    private static final class SetWrites implements SetWriteOps {
        private final YierdisDb db;

        private SetWrites(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long sadd(byte[] keyBytes, List<byte[]> members) {
            return db.sadd(keyBytes, members);
        }

        @Override
        public long srem(byte[] keyBytes, List<byte[]> members) {
            return db.srem(keyBytes, members);
        }
    }

    private static final class ZSetWrites implements ZSetWriteOps {
        private final YierdisDb db;

        private ZSetWrites(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
            return db.zadd(keyBytes, scoreMemberPairs);
        }

        @Override
        public long zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
            return db.zremrangeByScore(keyBytes, min, minExclusive, max, maxExclusive);
        }

        @Override
        public long zremrangeByRank(byte[] keyBytes, long start, long stop) {
            return db.zremrangeByRank(keyBytes, start, stop);
        }

        @Override
        public long zrem(byte[] keyBytes, List<byte[]> members) {
            return db.zrem(keyBytes, members);
        }
    }

    private static final class HllWrites implements HllWriteOps {
        private final YierdisDb db;

        private HllWrites(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public int pfadd(byte[] keyBytes, List<byte[]> elements) {
            return db.pfadd(keyBytes, elements);
        }

        @Override
        public void pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys) {
            db.pfmerge(destKeyBytes, sourceKeys);
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
