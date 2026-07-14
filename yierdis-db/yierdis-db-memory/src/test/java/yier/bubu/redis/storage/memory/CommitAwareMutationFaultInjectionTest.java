package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.command.ByteArrayCommandRecord;
import yier.bubu.redis.common.command.CommandRecordScope;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.DbCommitReservation;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.SetMode;

public class CommitAwareMutationFaultInjectionTest {
    @Test
    public void changedPreparedWriteReservesBeforeCommitAndPublishesExactlyOnce() {
        YierdisDb db = new YierdisDb();
        try {
            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 7);
            db.bindToCurrentThread();
            try (ImmutableCommandRecord record = record("SET", "key", "value");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertTrue(db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null).value());
            }

            Assert.assertEquals(1, publisher.reserved.get());
            Assert.assertEquals(1, publisher.published.get());
            Assert.assertEquals(7, publisher.dbIndex);
            Assert.assertEquals(DbCommitKind.USER, publisher.kind);
            Assert.assertEquals("SET", new String(publisher.record.toByteArray(0), StandardCharsets.US_ASCII));
            publisher.closeRetainedRecord();
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void enabledPublisherRejectsUnscopedChangedWriteBeforeStorageVisibility() {
        YierdisDb db = new YierdisDb();
        try {
            db.attachCommitPublisher(new RecordingPublisher(), 0);
            db.bindToCurrentThread();

            Assert.assertThrows(
                    DbCommitStreamUnavailableException.class,
                    () -> db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null)
            );
            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("key"))));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void failedPublisherRejectsBeforeAChangedOrNoopWriteCanPrepare() {
        YierdisDb db = new YierdisDb();
        try {
            RecordingPublisher publisher = new RecordingPublisher();
            publisher.available = false;
            db.attachCommitPublisher(publisher, 0);
            db.bindToCurrentThread();
            try (ImmutableCommandRecord record = record("SET", "key", "value");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertThrows(
                        DbCommitStreamUnavailableException.class,
                        () -> db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null)
                );
            }
            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("key"))));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void scopedNoopDoesNotReserveOrPublishAnEvent() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(bytes("key"), bytes("old"), SetMode.NORMAL, null);

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 0);
            try (ImmutableCommandRecord record = record("SET", "key", "new");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertFalse(db.writes().strings().setString(bytes("key"), bytes("new"), SetMode.NX, null).value());
            }

            Assert.assertEquals(0, publisher.reserved.get());
            Assert.assertEquals(0, publisher.published.get());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void delUsesOnePreparedCommitForUniqueLiveKeys() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(bytes("first"), bytes("one"), SetMode.NORMAL, null);
            db.writes().strings().setString(bytes("second"), bytes("two"), SetMode.NORMAL, null);

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 3);
            try (ImmutableCommandRecord record = record("DEL", "first", "first", "second", "missing");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertEquals(
                        2L,
                        db.writes().keyspace().del(java.util.List.of(
                                bytes("first"),
                                bytes("first"),
                                bytes("second"),
                                bytes("missing")
                        )).value().longValue()
                );
            }

            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("first"))));
            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("second"))));
            Assert.assertEquals(1, publisher.reserved.get());
            Assert.assertEquals(1, publisher.published.get());
            Assert.assertEquals(3, publisher.dbIndex);
            Assert.assertEquals(DbCommitKind.USER, publisher.kind);
            Assert.assertEquals("DEL", new String(publisher.record.toByteArray(0), StandardCharsets.US_ASCII));
            publisher.closeRetainedRecord();
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void zsetRemovalsUsePreparedCommits() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().zsets().zadd(bytes("zset"), java.util.List.of(
                    bytes("1"), bytes("a"),
                    bytes("2"), bytes("b"),
                    bytes("3"), bytes("c")
            ));

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 1);
            try (ImmutableCommandRecord record = record("ZREM", "zset", "a");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertEquals(1L, db.writes().zsets().zrem(bytes("zset"), java.util.List.of(bytes("a"))).value().longValue());
            }
            try (ImmutableCommandRecord record = record("ZREMRANGEBYRANK", "zset", "0", "0");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertEquals(1L, db.writes().zsets().zremrangeByRank(bytes("zset"), 0L, 0L).value().longValue());
            }
            try (ImmutableCommandRecord record = record("ZREMRANGEBYSCORE", "zset", "3", "3");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertEquals(
                        1L,
                        db.writes().zsets().zremrangeByScore(bytes("zset"), 3.0d, false, 3.0d, false).value().longValue()
                );
            }

            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("zset"))));
            Assert.assertEquals(3, publisher.reserved.get());
            Assert.assertEquals(3, publisher.published.get());
            Assert.assertEquals(1, publisher.dbIndex);
            Assert.assertEquals(DbCommitKind.USER, publisher.kind);
            Assert.assertEquals("ZREMRANGEBYSCORE", new String(publisher.record.toByteArray(0), StandardCharsets.US_ASCII));
            publisher.closeRetainedRecord();
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void flushDbUsesTheDatabaseCommitBoundary() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null);

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 5);
            try (ImmutableCommandRecord record = record("FLUSHDB");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertEquals(yier.bubu.redis.storage.api.MutationOutcome.VALUE_CHANGED, db.lifecycle().flushDb());
            }

            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("key"))));
            Assert.assertEquals(1, publisher.reserved.get());
            Assert.assertEquals(1, publisher.published.get());
            Assert.assertEquals(5, publisher.dbIndex);
            Assert.assertEquals(DbCommitKind.USER, publisher.kind);
            Assert.assertEquals("FLUSHDB", new String(publisher.record.toByteArray(0), StandardCharsets.US_ASCII));
            publisher.closeRetainedRecord();
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void reservationFailureAbortsPreparedWriteBeforeStorageVisibility() {
        YierdisDb db = new YierdisDb();
        try {
            RecordingPublisher publisher = new RecordingPublisher();
            publisher.failOnReserve = true;
            db.attachCommitPublisher(publisher, 0);
            db.bindToCurrentThread();
            try (ImmutableCommandRecord record = record("SET", "key", "value");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertThrows(
                        DbCommitStreamUnavailableException.class,
                        () -> db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null)
                );
            }

            Assert.assertEquals(1, publisher.reserved.get());
            Assert.assertEquals(0, publisher.published.get());
            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("key"))));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void publicationFailureKeepsCommittedStateAndDegradesTheDb() {
        YierdisDb db = new YierdisDb();
        try {
            RecordingPublisher publisher = new RecordingPublisher();
            publisher.failOnPublish = true;
            db.attachCommitPublisher(publisher, 0);
            db.bindToCurrentThread();
            try (ImmutableCommandRecord record = record("SET", "key", "value");
                 CommandRecordScope.Scope ignored = CommandRecordScope.open(record)) {
                Assert.assertThrows(
                        PostCommitMutationException.class,
                        () -> db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null)
                );
            }

            Assert.assertTrue(db.reads().keyspace().existsKey(view(bytes("key"))));
            Assert.assertEquals(1, publisher.reserved.get());
            Assert.assertEquals(1, publisher.published.get());
            Assert.assertEquals(1, publisher.failedAfterCommit.get());
            Assert.assertTrue(db.health().degraded());
            publisher.closeRetainedRecord();
        } finally {
            db.shutdown();
        }
    }

    private static ByteArrayCommandRecord record(String... argv) {
        byte[][] bytes = new byte[argv.length][];
        for (int i = 0; i < argv.length; i++) {
            bytes[i] = bytes(argv[i]);
        }
        return ByteArrayCommandRecord.copyOf(bytes);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static BytesView view(byte[] bytes) {
        return new BytesView() {
            @Override
            public int length() {
                return bytes.length;
            }

            @Override
            public byte getByte(int index) {
                return bytes[index];
            }
        };
    }

    private static final class RecordingPublisher implements DbCommitPublisher {
        private final AtomicInteger reserved = new AtomicInteger();
        private final AtomicInteger published = new AtomicInteger();
        private final AtomicInteger failedAfterCommit = new AtomicInteger();
        private boolean available = true;
        private boolean failOnReserve;
        private boolean failOnPublish;
        private int dbIndex = -1;
        private DbCommitKind kind;
        private ImmutableCommandRecord record;

        @Override
        public DbCommitReservation reserve(
                int dbIndex,
                DbCommitKind kind,
                ImmutableCommandRecord record,
                long committedMemoryDelta,
                long commitAttemptTimestampMillis
        ) {
            if (!available) {
                throw new DbCommitStreamUnavailableException();
            }
            this.dbIndex = dbIndex;
            this.kind = kind;
            reserved.incrementAndGet();
            if (failOnReserve) {
                throw new DbCommitStreamUnavailableException();
            }
            closeRetainedRecord();
            this.record = record.retain();
            return new DbCommitReservation() {
                @Override
                public long reservedMemoryBytes() {
                    return RecordingPublisher.this.record.retainedMemoryBytes();
                }

                @Override
                public boolean noop() {
                    return false;
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public long publish(DbCommitReservation reservation) {
            published.incrementAndGet();
            if (failOnPublish) {
                throw new IllegalStateException("injected publish failure");
            }
            return published.get();
        }

        @Override
        public void failAfterCommit(DbCommitReservation reservation) {
            failedAfterCommit.incrementAndGet();
        }

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public boolean available() {
            return available;
        }

        private void closeRetainedRecord() {
            if (record != null) {
                record.close();
            }
        }
    }
}
