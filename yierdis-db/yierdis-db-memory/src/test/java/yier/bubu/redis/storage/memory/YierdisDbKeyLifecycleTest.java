package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;

public class YierdisDbKeyLifecycleTest {
    @Test
    public void inspectionAndNullInputsHaveStableNoopContracts() {
        withDb(db -> {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();

            KeyLifecycleTestAccess.Inspection inspection = KeyLifecycleTestAccess.inspect(lifecycle);
            Assert.assertSame(KeyLifecycleTestAccess.backend(db), inspection.stableMemoryBackend());
            Assert.assertNotNull(inspection.entryTable());
            Assert.assertNotNull(inspection.keyDirectory());
            Assert.assertNotNull(inspection.stringRoot());
            Assert.assertNotNull(inspection.listRoot());
            Assert.assertNotNull(inspection.hashRoot());
            Assert.assertNotNull(inspection.setRoot());
            Assert.assertNotNull(inspection.zsetRoot());
            Assert.assertNull(lifecycle.keyHandle((byte[]) null));
            Assert.assertNull(lifecycle.keyHandle((BytesView) null));
            Assert.assertNull(lifecycle.entryHandle(null));
            Assert.assertNull(lifecycle.entryRecord((byte[]) null));
            Assert.assertNull(lifecycle.entryRecord((BytesView) null));
            Assert.assertNull(lifecycle.entryRecord((KeyHandle) null));
            Assert.assertNull(lifecycle.entryRecord((EntryHandle) null));
            Assert.assertNull(lifecycle.liveEntryRecord((byte[]) null));
            Assert.assertNull(lifecycle.liveEntryRecord((BytesView) null));
            Assert.assertNull(lifecycle.liveEntryRecord((KeyHandle) null));
            Assert.assertNull(lifecycle.copyKeyBytes(null));
            Assert.assertNull(lifecycle.expireAtMillis((byte[]) null));
            Assert.assertNull(lifecycle.expireAtMillis((KeyHandle) null));
            Assert.assertEquals(0L, lifecycle.estimatedBytesForRemoval(null, null));
            Assert.assertFalse(lifecycle.removeEntry(null, null));
            Assert.assertFalse(lifecycle.isKeyExpiredForScan(null, Long.MAX_VALUE));
            Assert.assertNull(lifecycle.unlinkEntry((byte[]) null));
            Assert.assertNull(lifecycle.unlinkEntry((EntryHandle) null));

            lifecycle.resetEntryStateCounters();
            Assert.assertEquals(0, lifecycle.expireCount());
            Assert.assertEquals(0L, lifecycle.expiredEntriesAwaitingPhysicalDeletion());
            Assert.assertThrows(NullPointerException.class,
                    () -> lifecycle.forEachKeyHandle(null));
            Assert.assertThrows(NullPointerException.class,
                    () -> lifecycle.stageEntry(null));
            Assert.assertThrows(NullPointerException.class,
                    () -> lifecycle.publishStagedEntry(null, null));
        });
    }

    @Test
    public void stagedEntryTokenAbortsOrPublishesOwnedHandlesExactlyOnce() {
        withDb(db -> {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            long baselineKeys = KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.KEY_BYTES);
            long baselineEntries = KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.ENTRY_RECORD);

            YierdisDbKeyLifecycle.StagedEntry aborted = lifecycle.stageEntry(bytes("aborted"));
            Assert.assertEquals(0, lifecycle.keyCount());
            Assert.assertEquals(baselineKeys + 1L,
                    KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(baselineEntries + 1L,
                    KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.ENTRY_RECORD));
            aborted.close();
            aborted.close();
            Assert.assertEquals(baselineKeys,
                    KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(baselineEntries,
                    KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.ENTRY_RECORD));

            byte[] publishedKey = bytes("published");
            YierdisDbKeyLifecycle.StagedEntry published = lifecycle.stageEntry(publishedKey);
            EntryRecord record = lifecycle.newRecord(
                    published.keyHandle(),
                    ValueHandle.NULL,
                    ValueType.STRING,
                    ValueEncoding.STRING_RAW,
                    -1L,
                    null
            );
            lifecycle.publishStagedEntry(published, record);
            published.close();

            YierdisDbKeyLifecycle.CurrentEntry current = lifecycle.currentEntry(publishedKey);
            Assert.assertEquals(record, current.record());
            Assert.assertEquals(1, lifecycle.keyCount());
            Assert.assertThrows(IllegalStateException.class, published::keyHandle);

            lifecycle.deleteEntry(current.entryHandle(), current.record());
            Assert.assertNull(lifecycle.entryRecord(publishedKey));
            Assert.assertEquals(0, lifecycle.keyCount());
            Assert.assertEquals(baselineKeys,
                    KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(baselineEntries,
                    KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.ENTRY_RECORD));
        });
    }

    @Test
    public void rejectedStagedPublishReleasesTheUnpublishedKeyEntryAndValue() {
        withDb(db -> {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            KeyLifecycleTestAccess.Inspection inspection = KeyLifecycleTestAccess.inspect(lifecycle);
            long baselineKeys = KeyLifecycleTestAccess.backend(db).stats()
                    .objectCount(NativeObjectKind.KEY_BYTES);
            long baselineEntries = KeyLifecycleTestAccess.backend(db).stats()
                    .objectCount(NativeObjectKind.ENTRY_RECORD);
            long baselineStrings = KeyLifecycleTestAccess.backend(db).stats()
                    .objectCount(NativeObjectKind.STRING_BYTES);
            byte[] key = bytes("publish-conflict");

            YierdisDbKeyLifecycle.StagedEntry rejected = lifecycle.stageEntry(key);
            ValueHandle rejectedValue = inspection.stringRoot().store(bytes("rejected"));
            EntryRecord rejectedRecord = lifecycle.newRecord(
                    rejected.keyHandle(),
                    rejectedValue,
                    ValueType.STRING,
                    ValueEncoding.STRING_RAW,
                    -1L,
                    null
            );
            YierdisDbKeyLifecycle.StagedEntry winner = lifecycle.stageEntry(key);
            EntryRecord winnerRecord = lifecycle.newRecord(
                    winner.keyHandle(),
                    ValueHandle.NULL,
                    ValueType.STRING,
                    ValueEncoding.STRING_RAW,
                    -1L,
                    null
            );
            lifecycle.publishStagedEntry(winner, winnerRecord);

            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> lifecycle.publishStagedEntry(rejected, rejectedRecord)
            );

            Assert.assertThrows(IllegalStateException.class, rejected::keyHandle);
            Assert.assertEquals(baselineKeys + 1L, KeyLifecycleTestAccess.backend(db).stats()
                    .objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(baselineEntries + 1L, KeyLifecycleTestAccess.backend(db).stats()
                    .objectCount(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(baselineStrings, KeyLifecycleTestAccess.backend(db).stats()
                    .objectCount(NativeObjectKind.STRING_BYTES));

            lifecycle.deleteEntry(lifecycle.entryHandle(key), winnerRecord);
            Assert.assertEquals(baselineKeys, KeyLifecycleTestAccess.backend(db).stats()
                    .objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(baselineEntries, KeyLifecycleTestAccess.backend(db).stats()
                    .objectCount(NativeObjectKind.ENTRY_RECORD));
        });
    }

    @Test
    public void ttlUpdatesReplaceEntryRecordsAndKeepDerivedCountExact() {
        withDb(db -> {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            byte[] key = bytes("ttl-key");
            createNullValueEntry(lifecycle, key);
            KeyHandle keyHandle = lifecycle.keyHandle(key);
            EntryRecord before = lifecycle.entryRecord(key);
            long deadline = System.currentTimeMillis() + 60_000L;

            Assert.assertTrue(db.writes().ttl().expireAtMillis(view(key), deadline).value());

            EntryRecord expiring = lifecycle.entryRecord(key);
            Assert.assertEquals(Long.valueOf(deadline), lifecycle.expireAtMillis(keyHandle));
            Assert.assertEquals(deadline, expiring.expireAtMillis());
            Assert.assertEquals(before.version() + 1L, expiring.version());
            Assert.assertEquals(1, lifecycle.expireCount());
            Assert.assertFalse(lifecycle.isKeyExpired(keyHandle, deadline - 1L));
            Assert.assertTrue(lifecycle.isKeyExpiredForScan(keyHandle, deadline));

            Assert.assertTrue(db.writes().ttl().persist(view(key)).value());
            EntryRecord persistent = lifecycle.entryRecord(key);
            Assert.assertNull(lifecycle.expireAtMillis(keyHandle));
            Assert.assertEquals(-1L, persistent.expireAtMillis());
            Assert.assertEquals(expiring.version() + 1L, persistent.version());
            Assert.assertEquals(0, lifecycle.expireCount());

            Assert.assertTrue(db.writes().ttl().expireAtMillis(view(key), deadline).value());
            EntryRecord expiringAgain = lifecycle.entryRecord(key);
            Assert.assertEquals(1, lifecycle.expireCount());
            Assert.assertTrue(lifecycle.removeEntry(keyHandle, expiringAgain));
            Assert.assertEquals(0, lifecycle.keyCount());
            Assert.assertEquals(0, lifecycle.expireCount());
        });
    }

    @Test
    public void expectedRecordChecksAndUnlinkVariantsReleaseResourcesOnce() {
        withDb(db -> {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            byte[] firstKey = bytes("expected-record");
            createNullValueEntry(lifecycle, firstKey);
            KeyHandle keyHandle = lifecycle.keyHandle(firstKey);
            EntryRecord actual = lifecycle.entryRecord(firstKey);
            EntryRecord mismatch = lifecycle.withExpireAtMillis(keyHandle, actual, 123L);

            Assert.assertFalse(lifecycle.removeEntry(keyHandle, mismatch));
            Assert.assertNotNull(lifecycle.entryRecord(firstKey));
            Assert.assertTrue(lifecycle.removeEntry(keyHandle, actual));
            Assert.assertNull(lifecycle.keyHandle(firstKey));

            byte[] secondKey = bytes("unlink-bytes");
            createNullValueEntry(lifecycle, secondKey);
            Assert.assertNotNull(lifecycle.unlinkEntry(secondKey));
            Assert.assertNull(lifecycle.unlinkEntry(secondKey));

            byte[] thirdKey = bytes("unlink-handle");
            createNullValueEntry(lifecycle, thirdKey);
            EntryHandle thirdHandle = lifecycle.entryHandle(thirdKey);
            Assert.assertNotNull(lifecycle.unlinkEntry(thirdHandle));
            Assert.assertNull(lifecycle.unlinkEntry(thirdHandle));
            Assert.assertEquals(0, lifecycle.keyCount());
        });
    }

    private static void createNullValueEntry(YierdisDbKeyLifecycle lifecycle, byte[] key) {
        try (YierdisDbKeyLifecycle.StagedEntry stagedEntry = lifecycle.stageEntry(key)) {
            lifecycle.publishStagedEntry(
                    stagedEntry,
                    lifecycle.newRecord(
                            stagedEntry.keyHandle(),
                            ValueHandle.NULL,
                            ValueType.STRING,
                            ValueEncoding.STRING_RAW,
                            -1L,
                            null
                    )
            );
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static BytesView view(byte[] value) {
        return new ArrayBytesView(value);
    }

    private static void withDb(DbConsumer consumer) {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            consumer.accept(db);
        } finally {
            db.shutdown();
        }
    }

    @FunctionalInterface
    private interface DbConsumer {
        void accept(YierdisDb db);
    }

    private record ArrayBytesView(byte[] bytes) implements BytesView {
        @Override
        public int length() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            return bytes[index];
        }
    }
}
