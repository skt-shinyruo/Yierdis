package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.command.ByteArrayCommandRecord;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.DbCommitReservation;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;

public class CommitAwareMutationFaultInjectionTest {
    @Test
    public void changedPreparedWriteReservesBeforeCommitAndPublishesExactlyOnce() {
        YierdisDb db = TestDbSupport.open();
        try {
            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 7);
            db.bindToCurrentThread();
            try (ImmutableCommandRecord record = record("SET", "key", "value")) {
                Assert.assertTrue(writes(db, record).strings()
                        .setString(bytes("key"), bytes("value"), SetMode.NORMAL, null).value());
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
        YierdisDb db = TestDbSupport.open();
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
    public void failedContextualWriteDoesNotLeakItsMutationContext() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(bytes("counter"), bytes("not-a-number"), SetMode.NORMAL, null);

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 0);
            try (ImmutableCommandRecord record = record("INCRBY", "counter", "1")) {
                Assert.assertThrows(
                        YierdisCommandException.class,
                        () -> writes(db, record).strings().incrBy(bytes("counter"), 1L)
                );
            }

            Assert.assertThrows(
                    DbCommitStreamUnavailableException.class,
                    () -> db.writes().strings()
                            .setString(bytes("next"), bytes("value"), SetMode.NORMAL, null)
            );
            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("next"))));
            Assert.assertEquals(0, publisher.reserved.get());
            Assert.assertEquals(0, publisher.published.get());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void independentlyBoundWriteViewsKeepTheirOwnMutationContexts() {
        YierdisDb db = TestDbSupport.open();
        RecordingPublisher publisher = new RecordingPublisher();
        try {
            db.bindToCurrentThread();
            db.attachCommitPublisher(publisher, 0);
            try (ImmutableCommandRecord firstRecord = record("SET_FIRST", "first", "1");
                 MutationContext firstContext = MutationContext.of(firstRecord);
                 ImmutableCommandRecord secondRecord = record("SET_SECOND", "second", "2");
                 MutationContext secondContext = MutationContext.of(secondRecord)) {
                DbWrites first = db.writes().withMutationContext(firstContext);
                DbWrites second = db.writes().withMutationContext(secondContext);

                Assert.assertNotSame(first, second);
                Assert.assertTrue(first.strings()
                        .setString(bytes("first"), bytes("1"), SetMode.NORMAL, null).value());
                Assert.assertEquals("SET_FIRST", commandName(publisher.record));

                Assert.assertTrue(second.strings()
                        .setString(bytes("second"), bytes("2"), SetMode.NORMAL, null).value());
                Assert.assertEquals("SET_SECOND", commandName(publisher.record));
            }
        } finally {
            publisher.closeRetainedRecord();
            db.shutdown();
        }
    }

    @Test
    public void independentlyBoundLifecycleViewsKeepTheirOwnMutationContexts() {
        YierdisDb db = TestDbSupport.open();
        RecordingPublisher publisher = new RecordingPublisher();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null);
            db.attachCommitPublisher(publisher, 0);
            try (ImmutableCommandRecord firstRecord = record("FLUSHDB_FIRST");
                 MutationContext firstContext = MutationContext.of(firstRecord);
                 ImmutableCommandRecord secondRecord = record("FLUSHDB_SECOND");
                 MutationContext secondContext = MutationContext.of(secondRecord)) {
                DbLifecycleOps first = db.lifecycle().withMutationContext(firstContext);
                DbLifecycleOps second = db.lifecycle().withMutationContext(secondContext);

                Assert.assertNotSame(first, second);
                Assert.assertEquals(
                        yier.bubu.redis.storage.api.MutationOutcome.VALUE_CHANGED,
                        first.flushDb()
                );
                Assert.assertEquals("FLUSHDB_FIRST", commandName(publisher.record));
            }
        } finally {
            publisher.closeRetainedRecord();
            db.shutdown();
        }
    }

    @Test
    public void lifecycleContextBindingRejectsNonOwnerThread() throws Exception {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try (ImmutableCommandRecord record = record("FLUSHDB");
                 MutationContext context = MutationContext.of(record)) {
                Thread thread = Thread.ofPlatform().start(() -> {
                    try {
                        db.lifecycle().withMutationContext(context);
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                });
                thread.join();
            }

            Assert.assertTrue(failure.get() instanceof IllegalStateException);
            Assert.assertEquals("YierdisDb accessed from a non-owner thread", failure.get().getMessage());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void failedPublisherRejectsBeforeAChangedOrNoopWriteCanPrepare() {
        YierdisDb db = TestDbSupport.open();
        try {
            RecordingPublisher publisher = new RecordingPublisher();
            publisher.available = false;
            db.attachCommitPublisher(publisher, 0);
            db.bindToCurrentThread();
            try (ImmutableCommandRecord record = record("SET", "key", "value")) {
                Assert.assertThrows(
                        DbCommitStreamUnavailableException.class,
                        () -> writes(db, record).strings()
                                .setString(bytes("key"), bytes("value"), SetMode.NORMAL, null)
                );
            }
            Assert.assertFalse(db.reads().keyspace().existsKey(view(bytes("key"))));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void scopedNoopDoesNotReserveOrPublishAnEvent() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(bytes("key"), bytes("old"), SetMode.NORMAL, null);

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 0);
            try (ImmutableCommandRecord record = record("SET", "key", "new")) {
                Assert.assertFalse(writes(db, record).strings()
                        .setString(bytes("key"), bytes("new"), SetMode.NX, null).value());
            }

            Assert.assertEquals(0, publisher.reserved.get());
            Assert.assertEquals(0, publisher.published.get());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void delUsesOnePreparedCommitForUniqueLiveKeys() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(bytes("first"), bytes("one"), SetMode.NORMAL, null);
            db.writes().strings().setString(bytes("second"), bytes("two"), SetMode.NORMAL, null);

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 3);
            try (ImmutableCommandRecord record = record("DEL", "first", "first", "second", "missing")) {
                Assert.assertEquals(
                        2L,
                        writes(db, record).keyspace().del(java.util.List.of(
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
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.writes().zsets().zadd(bytes("zset"), java.util.List.of(
                    bytes("1"), bytes("a"),
                    bytes("2"), bytes("b"),
                    bytes("3"), bytes("c")
            ));

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 1);
            try (ImmutableCommandRecord record = record("ZREM", "zset", "a")) {
                Assert.assertEquals(1L, writes(db, record).zsets()
                        .zrem(bytes("zset"), java.util.List.of(bytes("a"))).value().longValue());
            }
            try (ImmutableCommandRecord record = record("ZREMRANGEBYRANK", "zset", "0", "0")) {
                Assert.assertEquals(1L, writes(db, record).zsets()
                        .zremrangeByRank(bytes("zset"), 0L, 0L).value().longValue());
            }
            try (ImmutableCommandRecord record = record("ZREMRANGEBYSCORE", "zset", "3", "3")) {
                Assert.assertEquals(
                        1L,
                        writes(db, record).zsets()
                                .zremrangeByScore(bytes("zset"), 3.0d, false, 3.0d, false)
                                .value().longValue()
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
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null);

            RecordingPublisher publisher = new RecordingPublisher();
            db.attachCommitPublisher(publisher, 5);
            try (ImmutableCommandRecord record = record("FLUSHDB")) {
                Assert.assertEquals(
                        yier.bubu.redis.storage.api.MutationOutcome.VALUE_CHANGED,
                        db.lifecycle().withMutationContext(MutationContext.of(record)).flushDb()
                );
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
        YierdisDb db = TestDbSupport.open();
        try {
            RecordingPublisher publisher = new RecordingPublisher();
            publisher.failOnReserve = true;
            db.attachCommitPublisher(publisher, 0);
            db.bindToCurrentThread();
            try (ImmutableCommandRecord record = record("SET", "key", "value")) {
                Assert.assertThrows(
                        DbCommitStreamUnavailableException.class,
                        () -> writes(db, record).strings()
                                .setString(bytes("key"), bytes("value"), SetMode.NORMAL, null)
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
        YierdisDb db = TestDbSupport.open();
        try {
            RecordingPublisher publisher = new RecordingPublisher();
            publisher.failOnPublish = true;
            db.attachCommitPublisher(publisher, 0);
            db.bindToCurrentThread();
            try (ImmutableCommandRecord record = record("SET", "key", "value")) {
                Assert.assertThrows(
                        PostCommitMutationException.class,
                        () -> writes(db, record).strings()
                                .setString(bytes("key"), bytes("value"), SetMode.NORMAL, null)
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

    @Test
    public void publicationFailureStillReleasesSupersededListBlock() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            db.writes().lists().rpush(bytes("list"), java.util.List.of(bytes("old")));
            Assert.assertEquals(
                    1L,
                    db.stableMemoryBackend().stats().objectCount(NativeObjectKind.LISTPACK_BYTES)
            );

            RecordingPublisher publisher = new RecordingPublisher();
            publisher.failOnPublish = true;
            db.attachCommitPublisher(publisher, 0);
            try (ImmutableCommandRecord record = record("RPUSH", "list", "next")) {
                Assert.assertThrows(
                        PostCommitMutationException.class,
                        () -> writes(db, record).lists().rpush(
                                bytes("list"),
                                java.util.List.of(bytes("next"))
                        )
                );
            }

            Assert.assertEquals(
                    1L,
                    db.stableMemoryBackend().stats().objectCount(NativeObjectKind.LISTPACK_BYTES)
            );
            Assert.assertEquals(1, publisher.failedAfterCommit.get());
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

    private static String commandName(ImmutableCommandRecord record) {
        return new String(record.toByteArray(0), StandardCharsets.US_ASCII);
    }

    private static yier.bubu.redis.storage.api.DbWrites writes(
            YierdisDb db,
            ImmutableCommandRecord record
    ) {
        return db.writes().withMutationContext(MutationContext.of(record));
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
