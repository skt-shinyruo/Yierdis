package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class YierdisDbKeyLifecycleTest {
    @Test
    public void accessorsAndNullInputsHaveStableNoopContracts() {
        withDb(db -> {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();

            Assert.assertSame(db.stableMemoryBackend(), lifecycle.stableMemoryBackend());
            Assert.assertNotNull(lifecycle.entryTable());
            Assert.assertNotNull(lifecycle.keyDirectory());
            Assert.assertNotNull(lifecycle.stringRoot());
            Assert.assertNotNull(lifecycle.listRoot());
            Assert.assertNotNull(lifecycle.hashRoot());
            Assert.assertNotNull(lifecycle.setRoot());
            Assert.assertNotNull(lifecycle.zsetRoot());
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
            Assert.assertEquals(0L, lifecycle.estimateExpireSetUpperBound(null, true));
            Assert.assertEquals(0L, lifecycle.estimatedBytesForRemoval(null, null));
            Assert.assertFalse(lifecycle.removeEntry(null, null));
            Assert.assertFalse(lifecycle.isKeyExpiredForScan(null, Long.MAX_VALUE));
            Assert.assertNull(lifecycle.unlinkEntry((byte[]) null));
            Assert.assertNull(lifecycle.unlinkEntry((EntryHandle) null));

            PreparedTtlMutation set = lifecycle.prepareSetExpireAtMillis(null, 1L);
            PreparedTtlMutation remove = lifecycle.prepareRemoveExpire(null);
            Assert.assertSame(PreparedTtlMutation.NONE, set);
            Assert.assertSame(PreparedTtlMutation.NONE, remove);
            set.commit();
            set.releaseSuperseded();
            set.abort();
            remove.close();

            lifecycle.setExpireAtMillis((KeyHandle) null, 1L);
            lifecycle.removeExpire((KeyHandle) null);
            lifecycle.removeExpireIndexOnly(null);
            lifecycle.removeExpireByKeyBytes(null);
            lifecycle.resetExpiredEntriesAwaitingPhysicalDeletion();
            Assert.assertEquals(0L, lifecycle.expiredEntriesAwaitingPhysicalDeletion());
            Assert.assertThrows(NullPointerException.class,
                    () -> lifecycle.forEachKeyHandle(null));
            Assert.assertThrows(NullPointerException.class,
                    () -> lifecycle.computeWithHandleResult(null, (key, old) -> null));
            Assert.assertThrows(NullPointerException.class,
                    () -> lifecycle.computeWithHandleResult(bytes("k"), null));
            Assert.assertThrows(NullPointerException.class,
                    () -> lifecycle.computeIfPresentWithHandleResult(null, (key, old) -> null));
            Assert.assertThrows(NullPointerException.class,
                    () -> lifecycle.computeIfPresentWithHandleResult(bytes("k"), null));
        });
    }

    @Test
    public void computeCreatesReplacesRetainsAndDeletesOwnedValues() {
        withDb(db -> {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            long baselineStrings = db.stableMemoryBackend().stats().objectCount(NativeObjectKind.STRING_BYTES);
            byte[] key = bytes("compute-key");

            String created = lifecycle.computeWithHandleResult(key, (keyHandle, oldRecord) -> {
                Assert.assertNull(oldRecord);
                ValueHandle value = lifecycle.stringRoot().store(bytes("one"));
                EntryRecord record = lifecycle.newRecord(
                        keyHandle,
                        value,
                        ValueType.STRING,
                        ValueEncoding.STRING_RAW,
                        -1L,
                        null
                );
                return YierdisDbKeyLifecycle.EntryMutationResult.of(record, "created");
            });

            Assert.assertEquals("created", created);
            Assert.assertEquals(1, lifecycle.keyCount());
            EntryRecord firstRecord = lifecycle.entryRecord(key);
            ValueHandle firstValue = firstRecord.valueHandle();
            Assert.assertArrayEquals(bytes("one"), lifecycle.stringRoot().copy(firstValue));

            ValueHandle secondValue = lifecycle.stringRoot().store(bytes("two"));
            String replaced = lifecycle.computeWithHandleResult(key, (keyHandle, oldRecord) -> {
                EntryRecord replacement = lifecycle.newRecord(
                        keyHandle,
                        secondValue,
                        ValueType.STRING,
                        ValueEncoding.STRING_RAW,
                        -1L,
                        oldRecord
                );
                return YierdisDbKeyLifecycle.EntryMutationResult.of(replacement, "retained", false);
            });

            Assert.assertEquals("retained", replaced);
            Assert.assertTrue(lifecycle.stringRoot().contains(firstValue));
            Assert.assertArrayEquals(bytes("two"), lifecycle.stringRoot().copy(secondValue));
            lifecycle.stringRoot().release(firstValue);

            AtomicInteger invocations = new AtomicInteger();
            Assert.assertNull(lifecycle.computeIfPresentWithHandleResult(
                    bytes("missing"),
                    (keyHandle, oldRecord) -> {
                        invocations.incrementAndGet();
                        return YierdisDbKeyLifecycle.EntryMutationResult.of(null, "unexpected");
                    }
            ));
            Assert.assertEquals(0, invocations.get());

            String deleted = lifecycle.computeIfPresentWithHandleResult(
                    key,
                    (keyHandle, oldRecord) -> YierdisDbKeyLifecycle.EntryMutationResult.of(null, "deleted")
            );
            Assert.assertEquals("deleted", deleted);
            Assert.assertNull(lifecycle.entryRecord(key));
            Assert.assertEquals(0, lifecycle.keyCount());
            Assert.assertEquals(
                    baselineStrings,
                    db.stableMemoryBackend().stats().objectCount(NativeObjectKind.STRING_BYTES)
            );

            String absent = lifecycle.computeWithHandleResult(
                    bytes("not-published"),
                    (keyHandle, oldRecord) -> new YierdisDbKeyLifecycle.EntryMutationResult<>(null, "absent")
            );
            Assert.assertEquals("absent", absent);
            Assert.assertEquals(0, lifecycle.keyCount());
        });
    }

    @Test
    public void ttlUpdatesMirrorEntryRecordsAndByteFallbackRemovesDanglingExpiry() {
        withDb(db -> {
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            byte[] key = bytes("ttl-key");
            createNullValueEntry(lifecycle, key);
            KeyHandle keyHandle = lifecycle.keyHandle(key);
            EntryRecord before = lifecycle.entryRecord(key);
            long deadline = System.currentTimeMillis() + 60_000L;

            lifecycle.setExpireAtMillis(keyHandle, deadline);

            EntryRecord expiring = lifecycle.entryRecord(key);
            Assert.assertEquals(Long.valueOf(deadline), lifecycle.expireAtMillis(keyHandle));
            Assert.assertEquals(deadline, expiring.expireAtMillis());
            Assert.assertEquals(before.version() + 1L, expiring.version());
            Assert.assertFalse(lifecycle.isKeyExpired(keyHandle, deadline - 1L));
            Assert.assertTrue(lifecycle.isKeyExpiredForScan(keyHandle, deadline));

            lifecycle.removeExpire(key);
            EntryRecord persistent = lifecycle.entryRecord(key);
            Assert.assertNull(lifecycle.expireAtMillis(keyHandle));
            Assert.assertEquals(-1L, persistent.expireAtMillis());
            Assert.assertEquals(expiring.version() + 1L, persistent.version());

            byte[] detachedKey = bytes("detached-ttl");
            try (var staged = lifecycle.keyDirectory().stageInsert(detachedKey)) {
                KeyHandle detachedHandle = staged.keyHandle();
                lifecycle.setExpireAtMillis(detachedHandle, deadline);
                Assert.assertEquals(Long.valueOf(deadline), lifecycle.expireAtMillis(detachedHandle));

                lifecycle.removeExpire(detachedKey);

                Assert.assertNull(lifecycle.expireAtMillis(detachedHandle));
            }
            Assert.assertTrue(lifecycle.removeEntry(keyHandle, persistent));
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
        lifecycle.computeWithHandleResult(key, (keyHandle, oldRecord) ->
                YierdisDbKeyLifecycle.EntryMutationResult.of(
                        lifecycle.newRecord(
                                keyHandle,
                                ValueHandle.NULL,
                                ValueType.STRING,
                                ValueEncoding.STRING_RAW,
                                -1L,
                                oldRecord
                        ),
                        null
                )
        );
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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
}
