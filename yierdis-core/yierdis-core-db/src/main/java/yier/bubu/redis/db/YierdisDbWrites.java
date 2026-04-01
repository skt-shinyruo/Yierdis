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
    private final StringWriteOps strings;
    private final HashWriteOps hashes;
    private final ListWriteOps lists;
    private final SetWriteOps sets;
    private final ZSetWriteOps zsets;
    private final HllWriteOps hll;
    private final KeyspaceWriteOps keyspace;
    private final TtlWriteOps ttl;

    YierdisDbWrites(YierdisDb db, YierdisStringOps strings, YierdisKeyspaceOps keyspace, YierdisTtlOps ttl) {
        YierdisDb engine = Objects.requireNonNull(db, "db");
        this.strings = Objects.requireNonNull(strings, "strings");
        this.hashes = new HashWrites(engine);
        this.lists = new ListWrites(engine);
        this.sets = new SetWrites(engine);
        this.zsets = new ZSetWrites(engine);
        this.hll = new HllWrites(engine);
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

}
