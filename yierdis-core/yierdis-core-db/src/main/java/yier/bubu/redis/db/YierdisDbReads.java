package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.HashOps;
import yier.bubu.redis.ops.HashReadOps;
import yier.bubu.redis.ops.HllOps;
import yier.bubu.redis.ops.HllReadOps;
import yier.bubu.redis.ops.KeyspaceOps;
import yier.bubu.redis.ops.KeyspaceReadOps;
import yier.bubu.redis.ops.ListOps;
import yier.bubu.redis.ops.ListReadOps;
import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.ops.SetOps;
import yier.bubu.redis.ops.SetReadOps;
import yier.bubu.redis.ops.StringOps;
import yier.bubu.redis.ops.StringReadOps;
import yier.bubu.redis.ops.TtlOps;
import yier.bubu.redis.ops.TtlReadOps;
import yier.bubu.redis.ops.ValueOps;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.ZSetOps;
import yier.bubu.redis.ops.ZSetReadOps;
import yier.bubu.redis.ops.result.BulkStringMapPairs;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.ops.result.BulkStringValue;

import java.util.List;
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

    YierdisDbReads(ValueOps values, KeyspaceOps keyspaceOps, TtlOps ttlOps) {
        Objects.requireNonNull(values, "values");
        this.strings = new StringReads(values.strings());
        this.hashes = new HashReads(values.hashes());
        this.lists = new ListReads(values.lists());
        this.sets = new SetReads(values.sets());
        this.zsets = new ZSetReads(values.zsets());
        this.hll = new HllReads(values.hll());
        this.keyspace = new KeyspaceReads(keyspaceOps);
        this.ttl = new TtlReads(ttlOps);
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

    private static final class StringReads implements StringReadOps {
        private final StringOps delegate;

        private StringReads(StringOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public byte[] getStringBytes(byte[] keyBytes) {
            return delegate.getStringBytes(keyBytes);
        }

        @Override
        public BulkStringValue getStringValue(BytesView keyView) {
            return delegate.getStringValue(keyView);
        }

        @Override
        public long strlen(BytesView keyView) {
            return delegate.strlen(keyView);
        }

        @Override
        public int getBit(BytesView keyView, long offset) {
            return delegate.getBit(keyView, offset);
        }

        @Override
        public long bitcount(BytesView keyView) {
            return delegate.bitcount(keyView);
        }

        @Override
        public long bitcount(BytesView keyView, long start, long end) {
            return delegate.bitcount(keyView, start, end);
        }
    }

    private static final class HashReads implements HashReadOps {
        private final HashOps delegate;

        private HashReads(HashOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
            return delegate.hget(keyBytes, fieldBytes);
        }

        @Override
        public BulkStringMapPairs hgetall(byte[] keyBytes) {
            return delegate.hgetall(keyBytes);
        }

        @Override
        public long hlen(byte[] keyBytes) {
            return delegate.hlen(keyBytes);
        }
    }

    private static final class ListReads implements ListReadOps {
        private final ListOps delegate;

        private ListReads(ListOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public BulkStringSequence lrange(byte[] keyBytes, int start, int stop) {
            return delegate.lrange(keyBytes, start, stop);
        }
    }

    private static final class SetReads implements SetReadOps {
        private final SetOps delegate;

        private SetReads(SetOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public BulkStringSequence smembers(byte[] keyBytes) {
            return delegate.smembers(keyBytes);
        }

        @Override
        public boolean sismember(byte[] keyBytes, byte[] member) {
            return delegate.sismember(keyBytes, member);
        }

        @Override
        public long scard(byte[] keyBytes) {
            return delegate.scard(keyBytes);
        }
    }

    private static final class ZSetReads implements ZSetReadOps {
        private final ZSetOps delegate;

        private ZSetReads(ZSetOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public BulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
            return delegate.zrange(keyBytes, start, stop, withScores);
        }

        @Override
        public BulkStringSequence zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
            return delegate.zrevrange(keyBytes, start, stop, withScores);
        }

        @Override
        public BulkStringSequence zrangeByScore(
                byte[] keyBytes,
                double min,
                boolean minExclusive,
                double max,
                boolean maxExclusive,
                boolean withScores,
                long offset,
                long count
        ) {
            return delegate.zrangeByScore(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count);
        }

        @Override
        public BulkStringSequence zrevrangeByScore(
                byte[] keyBytes,
                double min,
                boolean minExclusive,
                double max,
                boolean maxExclusive,
                boolean withScores,
                long offset,
                long count
        ) {
            return delegate.zrevrangeByScore(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
    }

    private static final class HllReads implements HllReadOps {
        private final HllOps delegate;

        private HllReads(HllOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public long pfcount(List<byte[]> keys) {
            return delegate.pfcount(keys);
        }
    }

    private static final class KeyspaceReads implements KeyspaceReadOps {
        private final KeyspaceOps delegate;

        private KeyspaceReads(KeyspaceOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public ValueType typeOf(BytesView keyView) {
            return delegate.typeOf(keyView);
        }

        @Override
        public boolean existsKey(BytesView keyView) {
            return delegate.existsKey(keyView);
        }

        @Override
        public List<byte[]> keys(byte[] globPattern, int maxMatches, long timeBudgetNanos) {
            return delegate.keys(globPattern, maxMatches, timeBudgetNanos);
        }

        @Override
        public ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out) {
            return delegate.scan(cursor, globPattern, count, out);
        }
    }

    private static final class TtlReads implements TtlReadOps {
        private final TtlOps delegate;

        private TtlReads(TtlOps delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public long ttlSeconds(BytesView keyView) {
            return delegate.ttlSeconds(keyView);
        }

        @Override
        public long ttlMillis(BytesView keyView) {
            return delegate.ttlMillis(keyView);
        }
    }
}
