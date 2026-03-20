package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.HashReadOps;
import yier.bubu.redis.ops.HllReadOps;
import yier.bubu.redis.ops.KeyspaceReadOps;
import yier.bubu.redis.ops.ListReadOps;
import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.ops.SetReadOps;
import yier.bubu.redis.ops.StringReadOps;
import yier.bubu.redis.ops.TtlReadOps;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.ZSetReadOps;
import yier.bubu.redis.ops.result.BulkStringMapPairs;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.ops.result.BulkStringSequences;
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

    YierdisDbReads(YierdisDb db) {
        YierdisDb engine = Objects.requireNonNull(db, "db");
        this.strings = new StringReads(engine);
        this.hashes = new HashReads(engine);
        this.lists = new ListReads(engine);
        this.sets = new SetReads(engine);
        this.zsets = new ZSetReads(engine);
        this.hll = new HllReads(engine);
        this.keyspace = new KeyspaceReads(engine);
        this.ttl = new TtlReads(engine);
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
        private final YierdisDb db;

        private StringReads(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public byte[] getStringBytes(byte[] keyBytes) {
            return db.getStringBytes(keyBytes);
        }

        @Override
        public BulkStringValue getStringValue(BytesView keyView) {
            return db.getStringValue(keyView);
        }

        @Override
        public long strlen(BytesView keyView) {
            return db.strlen(keyView);
        }

        @Override
        public int getBit(BytesView keyView, long offset) {
            return db.getBit(keyView, offset);
        }

        @Override
        public long bitcount(BytesView keyView) {
            return db.bitcount(keyView);
        }

        @Override
        public long bitcount(BytesView keyView, long start, long end) {
            return db.bitcount(keyView, start, end);
        }
    }

    private static final class HashReads implements HashReadOps {
        private final YierdisDb db;

        private HashReads(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
            return db.hget(keyBytes, fieldBytes);
        }

        @Override
        public BulkStringMapPairs hgetall(byte[] keyBytes) {
            return pairsOf(
                    () -> db.hgetallCount(keyBytes),
                    out -> db.hgetallWriteTo(keyBytes, out)
            );
        }

        @Override
        public long hlen(byte[] keyBytes) {
            return db.hlen(keyBytes);
        }
    }

    private static final class ListReads implements ListReadOps {
        private final YierdisDb db;

        private ListReads(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public BulkStringSequence lrange(byte[] keyBytes, int start, int stop) {
            return sequenceOf(
                    () -> db.lrangeCount(keyBytes, start, stop),
                    out -> db.lrangeWriteTo(keyBytes, start, stop, out)
            );
        }
    }

    private static final class SetReads implements SetReadOps {
        private final YierdisDb db;

        private SetReads(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public BulkStringSequence smembers(byte[] keyBytes) {
            return sequenceOf(
                    () -> db.smembersCount(keyBytes),
                    out -> db.smembersWriteTo(keyBytes, out)
            );
        }

        @Override
        public boolean sismember(byte[] keyBytes, byte[] member) {
            return db.sismember(keyBytes, member);
        }

        @Override
        public long scard(byte[] keyBytes) {
            return db.scard(keyBytes);
        }
    }

    private static final class ZSetReads implements ZSetReadOps {
        private final YierdisDb db;

        private ZSetReads(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public BulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
            return sequenceOf(
                    () -> db.zrangeCount(keyBytes, start, stop, withScores),
                    out -> db.zrangeWriteTo(keyBytes, start, stop, withScores, out)
            );
        }

        @Override
        public BulkStringSequence zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
            return sequenceOf(
                    () -> db.zrevrangeCount(keyBytes, start, stop, withScores),
                    out -> db.zrevrangeWriteTo(keyBytes, start, stop, withScores, out)
            );
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
            return sequenceOf(
                    () -> db.zrangeByScoreCount(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count),
                    out -> db.zrangeByScoreWriteTo(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count, out)
            );
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
            return sequenceOf(
                    () -> db.zrevrangeByScoreCount(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count),
                    out -> db.zrevrangeByScoreWriteTo(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count, out)
            );
        }
    }

    private static final class HllReads implements HllReadOps {
        private final YierdisDb db;

        private HllReads(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long pfcount(List<byte[]> keys) {
            return db.pfcount(keys);
        }
    }

    private static final class KeyspaceReads implements KeyspaceReadOps {
        private final YierdisDb db;

        private KeyspaceReads(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public ValueType typeOf(BytesView keyView) {
            return db.typeOf(keyView);
        }

        @Override
        public boolean existsKey(BytesView keyView) {
            return db.existsKey(keyView);
        }

        @Override
        public List<byte[]> keys(byte[] globPattern, int maxMatches, long timeBudgetNanos) {
            return db.keys(globPattern, maxMatches, timeBudgetNanos);
        }

        @Override
        public ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out) {
            return db.scan(cursor, globPattern, count, out);
        }
    }

    private static final class TtlReads implements TtlReadOps {
        private final YierdisDb db;

        private TtlReads(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long ttlSeconds(BytesView keyView) {
            return db.ttlSeconds(keyView);
        }

        @Override
        public long ttlMillis(BytesView keyView) {
            return db.ttlMillis(keyView);
        }
    }

    private static BulkStringSequence sequenceOf(List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return BulkStringSequences.empty();
        }
        return new BulkStringSequence() {
            @Override
            public int count() {
                return values.size();
            }

            @Override
            public void emitTo(BulkStringSink out) {
                for (byte[] value : values) {
                    if (value == null) {
                        out.bulkStringNull();
                    } else {
                        out.bulkString(value, 0, value.length);
                    }
                }
            }
        };
    }

    private static BulkStringSequence sequenceOf(IntSupplier countSupplier, BulkEmitter emitter) {
        Objects.requireNonNull(countSupplier, "countSupplier");
        Objects.requireNonNull(emitter, "emitter");
        return new BulkStringSequence() {
            @Override
            public int count() {
                int count = countSupplier.getAsInt();
                return Math.max(count, 0);
            }

            @Override
            public void emitTo(BulkStringSink out) {
                emitter.emitTo(out);
            }
        };
    }

    private static BulkStringMapPairs pairsOf(List<byte[]> pairs) {
        if (pairs == null || pairs.isEmpty()) {
            return new BulkStringMapPairs() {
                @Override
                public int pairCount() {
                    return 0;
                }

                @Override
                public void emitPairsTo(BulkStringSink out) {
                }
            };
        }
        return new BulkStringMapPairs() {
            @Override
            public int pairCount() {
                return pairs.size() / 2;
            }

            @Override
            public void emitPairsTo(BulkStringSink out) {
                for (byte[] value : pairs) {
                    if (value == null) {
                        out.bulkStringNull();
                    } else {
                        out.bulkString(value, 0, value.length);
                    }
                }
            }
        };
    }

    private static BulkStringMapPairs pairsOf(IntSupplier countSupplier, BulkEmitter emitter) {
        Objects.requireNonNull(countSupplier, "countSupplier");
        Objects.requireNonNull(emitter, "emitter");
        return new BulkStringMapPairs() {
            @Override
            public int pairCount() {
                int count = countSupplier.getAsInt();
                return Math.max(count / 2, 0);
            }

            @Override
            public void emitPairsTo(BulkStringSink out) {
                emitter.emitTo(out);
            }
        };
    }

    @FunctionalInterface
    private interface IntSupplier {
        int getAsInt();
    }

    @FunctionalInterface
    private interface BulkEmitter {
        void emitTo(BulkStringSink out);
    }
}
