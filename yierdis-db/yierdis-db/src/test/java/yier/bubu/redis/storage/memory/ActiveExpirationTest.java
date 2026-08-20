package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.testkit.TestBytes.view;

public class ActiveExpirationTest {
    @Test
    public void cleanupExpiredRemovesImmediatelyExpiredKeysWithoutAccess() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        Assert.assertEquals(1, db.size());

        db.cleanupExpired();
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void staleExpirationCandidateIsRejectedAfterRecordChanges() {
        YierdisDb db = TestDbSupport.open();
        try {
            byte[] key = b("stale");
            db.strings().setString(key, b("value"), SetMode.NORMAL, ExpireOption.px(0));
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            AllocatorKeyHandle keyHandle = lifecycle.keyHandle(key);
            EntryRecord candidate = lifecycle.entryRecord(keyHandle);

            makePersistentWithoutStartingAnotherMutation(db, key);

            Assert.assertFalse(lifecycle.isCurrentExpiredCandidate(
                    key,
                    keyHandle,
                    candidate,
                    Long.MAX_VALUE
            ));
            Assert.assertEquals(1, db.size());
            Assert.assertEquals(0, db.memoryStats().expireCount());
            Assert.assertArrayEquals(b("value"), OwnedReplyValueAssertions.stringValue(db.strings(), key));
            Assert.assertEquals(-1L, db.ttl().ttlMillis(viewOf(key)));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredEventuallyRemovesManyExpiredKeysWithoutAccess() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        int n = 200;
        for (int i = 0; i < n; i++) {
            byte[] key = b("k" + i);
            db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        }
        Assert.assertEquals(n, db.size());

        for (int i = 0; i < 100 && db.size() > 0; i++) {
            db.cleanupExpired();
        }
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void cleanupExpiredCapsCandidatesAndAdvancesAcrossCalls() {
        YierdisDb db = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            for (int i = 0; i < 45; i++) {
                db.strings().setString(
                        b("bounded-" + i),
                        b("v"),
                        SetMode.NORMAL,
                        ExpireOption.px(0)
                );
            }

            db.cleanupExpired();

            Assert.assertEquals(25, db.size());
            Assert.assertEquals(25, db.memoryStats().expireCount());
            for (int i = 0; i < 10 && db.size() > 0; i++) {
                db.cleanupExpired();
            }
            Assert.assertEquals(0, db.size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredDeduplicatesRehashShadowCandidates() {
        YierdisDb db = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            NativeKeyDirectory directory = KeyLifecycleTestAccess.inspect(db.keyLifecycle()).keyDirectory();
            int inserted = 0;
            while (inserted < 200) {
                db.strings().setString(
                        b("rehash-expired-" + inserted),
                        b("v"),
                        SetMode.NORMAL,
                        ExpireOption.px(0)
                );
                inserted++;
                if (!directory.metrics().rehashing()) {
                    continue;
                }
                if (directory.metrics().oldCapacity() >= 32) {
                    break;
                }
                drainDirectoryRehash(directory);
            }
            Assert.assertTrue(directory.metrics().rehashing());
            int oldCapacity = directory.metrics().oldCapacity();
            directory.advanceRehash(HashTableWorkBudget.of(oldCapacity / 2L, Long.MAX_VALUE));
            Assert.assertTrue(directory.metrics().rehashing());
            Assert.assertTrue(countDuplicateScanIdentities(db) > 0);

            int sizeBeforeCleanup = db.size();

            db.cleanupExpired(Long.MAX_VALUE);

            Assert.assertEquals(20, sizeBeforeCleanup - db.size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredResetsCursorWhenFullTableGenerationChanges() throws Exception {
        YierdisDb db = TestDbSupport.open(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                Long.MAX_VALUE
        );
        try {
            for (int i = 0; i < 100; i++) {
                db.strings().setString(
                        b("generation-expired-" + i),
                        b("v"),
                        SetMode.NORMAL,
                        ExpireOption.px(0)
                );
            }
            NativeKeyDirectory directory = KeyLifecycleTestAccess.inspect(db.keyLifecycle()).keyDirectory();
            drainDirectoryRehash(directory);
            long scannedGeneration = directory.tableGeneration();
            ExpirationBatch firstBatch = scanExpiredBatch(db, ScanCursorV2.start(), 20);
            Assert.assertEquals(20, firstBatch.keys().size());
            Assert.assertNotEquals(0L, firstBatch.nextCursor().value());

            db.cleanupExpired(Long.MAX_VALUE);

            Assert.assertEquals(80, db.size());
            Assert.assertEquals(scannedGeneration, directory.tableGeneration());
            byte[] expectedFromStart = null;
            byte[] expectedFromRetainedCursor = null;
            for (int i = 0; i < 1_000; i++) {
                db.strings().setString(
                        b("generation-new-expired-" + i),
                        b("v"),
                        SetMode.NORMAL,
                        ExpireOption.px(0)
                );
                Assert.assertEquals(scannedGeneration, directory.tableGeneration());
                expectedFromStart = firstExpiredKey(db, ScanCursorV2.start());
                expectedFromRetainedCursor = firstExpiredKey(db, firstBatch.nextCursor());
                if (!Arrays.equals(expectedFromStart, expectedFromRetainedCursor)) {
                    break;
                }
            }
            Assert.assertNotNull(expectedFromStart);
            Assert.assertNotNull(expectedFromRetainedCursor);
            Assert.assertFalse(Arrays.equals(expectedFromStart, expectedFromRetainedCursor));

            int retainedWireGeneration = firstBatch.nextCursor().generation();
            advanceFullGenerationKeepingWireToken(directory);
            Assert.assertEquals(
                    retainedWireGeneration,
                    (int) (directory.tableGeneration() & 0x1fff_ffffL)
            );

            int sizeBeforeCleanup = db.size();
            db.cleanupExpired(Long.MAX_VALUE);

            Assert.assertEquals(20, sizeBeforeCleanup - db.size());
            Assert.assertNull(db.keyLifecycle().entryRecord(expectedFromStart));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredDoesNotDeleteUnexpiredKeys() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(60_000));

        db.cleanupExpired();

        Assert.assertEquals(1, db.size());
        Assert.assertArrayEquals(b("v"), OwnedReplyValueAssertions.stringValue(db.strings(), key));

        db.shutdown();
    }

    @Test
    public void ttlBytesViewLazilyDeletesExpiredKeys() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        Assert.assertEquals(1, db.size());

        BytesView view = viewOf(key);

        Assert.assertEquals(-2L, db.ttl().ttlSeconds(view));
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void deadlineOnlyMutationReusesStoredKeyAndDoesNotChangePhysicalMemoryAccounting() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        byte[] equalLookupKey = b("k");
        Assert.assertNotSame(key, equalLookupKey);
        db.strings().setString(key, b("v"), SetMode.NORMAL, null);
        long usedBeforeTtl = db.usedBytesForMaxmemory();
        BytesView keyView = viewOf(equalLookupKey);

        Assert.assertTrue(db.ttl().expire(keyView, 60).value());
        long usedAfterTtl = db.usedBytesForMaxmemory();
        Assert.assertEquals(usedBeforeTtl, usedAfterTtl);
        Assert.assertEquals(1, db.memoryStats().expireCount());

        Assert.assertTrue(db.ttl().expire(keyView, 120).value());
        Assert.assertEquals(usedAfterTtl, db.usedBytesForMaxmemory());
        Assert.assertTrue(db.ttl().persist(keyView).value());
        long usedAfterPersist = db.usedBytesForMaxmemory();
        Assert.assertEquals(0, db.memoryStats().expireCount());
        Assert.assertEquals(usedBeforeTtl, usedAfterPersist);

        db.shutdown();
    }

    @Test
    public void pexpireBytesViewReadsOnlyTheLookupKey() {
        YierdisDb db = TestDbSupport.open();
        try {
            byte[] targetKey = b("bytes-view-target");
            db.strings().setString(targetKey, b("v"), SetMode.NORMAL, null);
            for (int i = 0; i < 256; i++) {
                db.strings().setString(
                        b("bytes-view-dummy-" + i),
                        b("v"),
                        SetMode.NORMAL,
                        ExpireOption.px(60_000)
                );
            }
            int[] reads = new int[1];
            BytesView view = new BytesView() {
                @Override
                public int length() {
                    return targetKey.length;
                }

                @Override
                public byte getByte(int index) {
                    reads[0]++;
                    return targetKey[index];
                }
            };

            Assert.assertTrue(db.ttl().pexpire(view, 60_000L).value());

            Assert.assertEquals(targetKey.length, reads[0]);
            Assert.assertEquals(257, db.memoryStats().expireCount());
            Assert.assertTrue(db.ttl().ttlMillis(viewOf(targetKey)) > 0L);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void cleanupExpiredNowMillisHonorsArgument() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(60_000));
        Assert.assertEquals(1, db.size());

        long now = System.currentTimeMillis();
        long farFuture = now + 120_000L;
        db.cleanupExpired(farFuture);

        Assert.assertEquals(0, db.size());
        db.shutdown();
    }

    private static BytesView viewOf(byte[] data) {
        return view(data);
    }

    private static void makePersistentWithoutStartingAnotherMutation(YierdisDb db, byte[] key) {
        YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
        AllocatorKeyHandle keyHandle = lifecycle.keyHandle(key);
        EntryHandle entryHandle = lifecycle.entryHandle(key);
        EntryRecord record = lifecycle.entryRecord(entryHandle);
        Assert.assertNotNull(keyHandle);
        Assert.assertNotNull(entryHandle);
        Assert.assertNotNull(record);
        lifecycle.replaceEntry(entryHandle, record, lifecycle.withExpireAtMillis(keyHandle, record, -1L));
    }

    private static void drainDirectoryRehash(NativeKeyDirectory directory) {
        while (directory.metrics().rehashing()) {
            directory.advanceRehash(HashTableWorkBudget.of(Long.MAX_VALUE, Long.MAX_VALUE));
        }
    }

    private static int countDuplicateScanIdentities(YierdisDb db) {
        Set<NativeHandle> identities = new HashSet<>();
        int[] visibleEntries = new int[1];
        YierdisDbKeyLifecycle.KeyScanResult result = db.keyLifecycle().scanWithWork(
                ScanCursorV2.start(),
                Long.MAX_VALUE,
                (key, record) -> {
                    visibleEntries[0]++;
                    identities.add(record.keyHandle());
                    return true;
                }
        );
        Assert.assertEquals(0L, result.nextCursor().value());
        return visibleEntries[0] - identities.size();
    }

    private static ExpirationBatch scanExpiredBatch(YierdisDb db, ScanCursorV2 cursor, int limit) {
        List<byte[]> keys = new ArrayList<>(limit);
        Set<NativeHandle> identities = new HashSet<>(limit);
        YierdisDbKeyLifecycle.KeyScanResult result = db.keyLifecycle().scanWithWork(cursor, 320L, (key, record) -> {
            if (record != null
                    && record.expireAtMillis() >= 0L
                    && identities.add(record.keyHandle())) {
                keys.add(db.keyLifecycle().copyKeyBytes(key));
            }
            return keys.size() < limit;
        });
        return new ExpirationBatch(keys, result.nextCursor());
    }

    private static byte[] firstExpiredKey(YierdisDb db, ScanCursorV2 cursor) {
        ExpirationBatch batch = scanExpiredBatch(db, cursor, 1);
        return batch.keys().isEmpty() ? null : batch.keys().getFirst();
    }

    private static void advanceFullGenerationKeepingWireToken(NativeKeyDirectory directory) throws Exception {
        Field topologyField = NativeKeyDirectory.class.getDeclaredField("topology");
        topologyField.setAccessible(true);
        OpenAddressingTopology topology = (OpenAddressingTopology) topologyField.get(directory);
        Field generation = OpenAddressingTopology.class.getDeclaredField("generation");
        generation.setAccessible(true);
        long current = generation.getLong(topology);
        // wire cursor 只携带低 29 位；保持该部分不变，才能验证 cleaner 保存的完整 generation sidecar。
        generation.setLong(topology, Math.addExact(current, 1L << 29));
    }

    private record ExpirationBatch(List<byte[]> keys, ScanCursorV2 nextCursor) {
    }

}
