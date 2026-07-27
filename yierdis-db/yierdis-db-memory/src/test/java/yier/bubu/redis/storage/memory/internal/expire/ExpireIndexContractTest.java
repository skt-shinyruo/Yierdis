package yier.bubu.redis.storage.memory.internal.expire;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.expire.YierdisNativeExpireIndex;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkResult;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExpireIndexContractTest {
    private static final HashSeed FIXED_SEED = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);

    @Test
    public void expireIndexUsesOnlyStableMemoryApi() throws Exception {
        Class<?> type = Class.forName(
                "yier.bubu.redis.storage.memory.internal.expire.YierdisNativeExpireIndex"
        );
        Assert.assertThrows(
                ClassNotFoundException.class,
                () -> Class.forName(
                        "yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex"
                )
        );
        for (Field field : type.getDeclaredFields()) {
            String fieldType = field.getType().getName();
            Assert.assertFalse(fieldType, fieldType.contains("memory." + "foreign"));
        Assert.assertFalse(fieldType, fieldType.contains("java.lang." + "foreign"));
        }
    }

    @Test
    public void emptyIndexHasNoFootprintAndMissingKeyOperationsAreNoops() {
        try (TestBackend runtime = TestBackend.open("expire-empty-contract");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            long baselineRegions = allocator.liveRegionCount();
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            try {
                byte[] missing = bytes("missing");
                Assert.assertEquals(0, expires.size());
                Assert.assertEquals(0, expires.table0Capacity());
                Assert.assertEquals(0, expires.table1Capacity());
                Assert.assertEquals(0L, expires.nativeBytes());
                Assert.assertEquals(0L, expires.heapEstimatedBytes());
                Assert.assertEquals(baselineRegions, allocator.liveRegionCount());
                Assert.assertNull(expires.get(missing));
                Assert.assertNull(expires.get(view(missing)));
                Assert.assertNull(expires.randomKey());
                Assert.assertNull(expires.randomKeyHandle());
                Assert.assertFalse(expires.hasMaintenanceDebt());
                Assert.assertEquals(0L, expires.estimatedMaintenanceGrowthBytes());
                Assert.assertNull(expires.stageMaintenanceResize());
                Assert.assertNull(expires.prepareMaintenance());

                expires.removeExpire(missing);
                try (NativeKeyDirectory.StagedInsert staged = directory.stageInsert(missing)) {
                    Assert.assertSame(
                            PreparedTtlMutation.NONE,
                            expires.prepareRemoveExpire(staged.keyHandle())
                    );
                }
                HashTableWorkResult work = expires.advanceRehash(
                        HashTableWorkBudget.of(1L, Long.MAX_VALUE)
                );
                Assert.assertTrue(work.rehashComplete());
                Assert.assertEquals(HashTableWorkResult.StopReason.NOT_REHASHING, work.stopReason());
            } finally {
                expires.close();
            }
            Assert.assertEquals(baselineRegions, allocator.liveRegionCount());
        }
    }

    @Test
    public void preparedReplacementSupportsAbortCommitAndTerminalStateGuards() {
        try (TestBackend runtime = TestBackend.open("expire-prepared-replacement");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            List<NativeHandle> entries = new ArrayList<>();
            byte[] key = bytes("prepared-replacement");
            try {
                directory.compute(key, (ignored, old) -> new EntryHandle(allocateEntry(allocator, entries)));
                KeyHandle handle = directory.getKeyHandle(key);
                long baselineRegions = allocator.liveRegionCount();

                PreparedTtlMutation aborted = expires.prepareSetExpireAtMillis(handle, 100L);
                Assert.assertTrue(aborted.stagedNonNativeGrowthBytes() > 0L);
                Assert.assertTrue(allocator.liveRegionCount() > baselineRegions);
                aborted.releaseSuperseded();
                Assert.assertNull(expires.get(handle));
                aborted.abort();
                aborted.abort();
                Assert.assertEquals(baselineRegions, allocator.liveRegionCount());
                Assert.assertThrows(IllegalStateException.class, aborted::commit);

                PreparedTtlMutation inserted = expires.prepareSetExpireAtMillis(handle, 200L);
                inserted.commit();
                Assert.assertEquals(Long.valueOf(200L), expires.get(handle));
                Assert.assertThrows(IllegalStateException.class, inserted::commit);
                inserted.abort();
                inserted.releaseSuperseded();
                inserted.releaseSuperseded();

                PreparedTtlMutation replacement = expires.prepareSetExpireAtMillis(handle, 300L);
                replacement.commit();
                Assert.assertEquals(Long.valueOf(300L), expires.get(handle));
                replacement.releaseSuperseded();
            } finally {
                expires.clear();
                expires.close();
                freeEntries(allocator, entries);
            }
        }
    }

    @Test
    public void preparedRemovalRejectsAbortDoubleCommitAndStaleTargets() {
        try (TestBackend runtime = TestBackend.open("expire-prepared-removal-guards");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            List<NativeHandle> entries = new ArrayList<>();
            byte[] key = bytes("prepared-remove");
            try {
                directory.compute(key, (ignored, old) -> new EntryHandle(allocateEntry(allocator, entries)));
                KeyHandle handle = directory.getKeyHandle(key);
                expires.setExpireAtMillis(handle, 100L);

                PreparedTtlMutation aborted = expires.prepareRemoveExpire(handle);
                aborted.abort();
                aborted.abort();
                Assert.assertThrows(IllegalStateException.class, aborted::commit);
                Assert.assertEquals(Long.valueOf(100L), expires.get(handle));

                PreparedTtlMutation first = expires.prepareRemoveExpire(handle);
                PreparedTtlMutation stale = expires.prepareRemoveExpire(handle);
                first.releaseSuperseded();
                Assert.assertEquals(Long.valueOf(100L), expires.get(handle));
                first.commit();
                Assert.assertThrows(IllegalStateException.class, first::commit);
                Assert.assertThrows(IllegalStateException.class, stale::commit);
                stale.abort();
                first.releaseSuperseded();
                first.releaseSuperseded();

                Assert.assertNull(expires.get(handle));
                Assert.assertEquals(0, expires.size());
                Assert.assertSame(PreparedTtlMutation.NONE, expires.prepareRemoveExpire(handle));
                Assert.assertEquals(0, expires.table0Capacity());
                Assert.assertEquals(0, expires.table1Capacity());
            } finally {
                expires.close();
                freeEntries(allocator, entries);
            }
        }
    }

    @Test
    public void stagedMaintenanceAbortAndStalePublishReleaseTheirRegions() {
        try (TestBackend runtime = TestBackend.open("expire-staged-resize-guards");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            List<NativeHandle> entries = new ArrayList<>();
            List<byte[]> keys = new ArrayList<>();
            try {
                for (int index = 0; index < 64; index++) {
                    byte[] key = bytes("resize-guard-" + index);
                    keys.add(key);
                    directory.compute(key, (ignored, old) -> new EntryHandle(allocateEntry(allocator, entries)));
                    expires.setExpireAtMillis(directory.getKeyHandle(key), 1_000L + index);
                }
                drainRehash(expires);
                for (int index = 0; index < 60; index++) {
                    PreparedTtlMutation removal = expires.prepareRemoveExpire(
                            directory.getKeyHandle(keys.get(index))
                    );
                    removal.commit();
                    removal.releaseSuperseded();
                }
                long liveBeforeStaging = allocator.liveRegionCount();

                HashTableMaintenanceRegistry.MaintenancePreparation preparation = expires.prepareMaintenance();
                Assert.assertNotNull(preparation);
                Assert.assertTrue(preparation.stagedNonNativeGrowthBytes() > 0L);
                preparation.abort();
                preparation.abort();
                Assert.assertEquals(liveBeforeStaging, allocator.liveRegionCount());
                Assert.assertThrows(IllegalStateException.class, preparation::stagedNonNativeGrowthBytes);
                Assert.assertThrows(IllegalStateException.class, preparation::commit);

                YierdisNativeExpireIndex.StagedResize first = expires.stageMaintenanceResize();
                YierdisNativeExpireIndex.StagedResize stale = expires.stageMaintenanceResize();
                Assert.assertNotNull(first);
                Assert.assertNotNull(stale);
                first.stagedNonNativeGrowthBytes();
                expires.publishStagedResize(first);
                Assert.assertThrows(IllegalStateException.class, first::stagedNonNativeGrowthBytes);
                Assert.assertThrows(IllegalStateException.class, () -> expires.publishStagedResize(stale));
                stale.close();
                stale.close();
                drainRehash(expires);
            } finally {
                expires.clear();
                expires.close();
                freeEntries(allocator, entries);
            }
        }
    }

    @Test
    public void registryAdvancesOnlyTheExpiryIndexWithRehashDebtAndUnregistersItWhenComplete() {
        try (TestBackend runtime = TestBackend.open("ffm-expire-registry");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, registry);
            List<NativeHandle> entries = new ArrayList<>();
            try {
                for (int i = 0; i < 13; i++) {
                    byte[] key = bytes("registry-expire-" + i);
                    NativeHandle entry = allocateEntry(allocator, entries);
                    directory.compute(key, (ignored, old) -> new EntryHandle(entry));
                    expires.setExpireAtMillis(directory.getKeyHandle(key), 10_000L + i);
                }

                Assert.assertTrue(expires.metrics().rehashing());
                Assert.assertEquals(1, registry.pendingTableCount());

                HashTableMaintenanceResult firstTick = registry.advance(HashTableWorkBudget.of(3L, Long.MAX_VALUE));

                Assert.assertEquals(3L, firstTick.inspectedSlots());
                Assert.assertEquals(HashTableMaintenanceResult.StopReason.SLOT_LIMIT, firstTick.stopReason());
                while (registry.pendingTableCount() != 0) {
                    registry.advance(HashTableWorkBudget.of(4L, Long.MAX_VALUE));
                }
                Assert.assertFalse(expires.hasMaintenanceDebt());
                Assert.assertFalse(expires.metrics().rehashing());
            } finally {
                expires.clear();
                expires.close();
                freeEntries(allocator, entries);
            }
        }
    }

    @Test
    public void sparseRehashCountsEmptySlotsAgainstItsBudget() {
        try (TestBackend runtime = TestBackend.open("ffm-expire-rehash-budget");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            List<NativeHandle> entries = new ArrayList<>();
            try {
                for (int slot = 4; slot < 16; slot++) {
                    byte[] key = keyForInitialExpirySlot(slot);
                    NativeHandle entry = allocateEntry(allocator, entries);
                    directory.compute(key, (ignored, old) -> new EntryHandle(entry));
                    expires.setExpireAtMillis(directory.getKeyHandle(key), 1_000L + slot);
                }
                byte[] growingKey = keyForInitialExpirySlot(4, 1);
                NativeHandle entry = allocateEntry(allocator, entries);
                directory.compute(growingKey, (ignored, old) -> new EntryHandle(entry));
                expires.setExpireAtMillis(directory.getKeyHandle(growingKey), 2_000L);

                Assert.assertTrue(expires.metrics().rehashing());
                HashTableWorkResult work = expires.advanceRehash(HashTableWorkBudget.of(4L, Long.MAX_VALUE));
                Assert.assertEquals(4L, work.inspectedSlots());
                Assert.assertEquals(0L, work.migratedSlots());
                Assert.assertFalse(work.rehashComplete());
            } finally {
                expires.clear();
                expires.close();
                freeEntries(allocator, entries);
            }
        }
    }

    @Test
    public void expiryResizeTargetsFollowSharedCapacityPolicy() {
        try (TestBackend runtime = TestBackend.open("ffm-expire-capacity-policy");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            List<NativeHandle> entries = new ArrayList<>();
            List<byte[]> keys = new ArrayList<>();
            try {
                for (int i = 0; i < 13; i++) {
                    byte[] key = bytes("capacity-policy-" + i);
                    keys.add(key);
                    NativeHandle entry = allocateEntry(allocator, entries);
                    directory.compute(key, (ignored, old) -> new EntryHandle(entry));
                    expires.setExpireAtMillis(directory.getKeyHandle(key), 10_000L + i);
                }

                assertResize(expires, 32, 16);
                drainRehash(expires);

                for (int i = 0; i < 7; i++) {
                    expires.removeExpire(keys.get(i));
                }

                Assert.assertFalse(expires.metrics().rehashing());
                Assert.assertTrue(expires.hasMaintenanceDebt());
                Assert.assertEquals(32, expires.metrics().capacity());
                Assert.assertEquals(0, expires.metrics().oldCapacity());

                publishPendingMaintenance(expires);
                assertResize(expires, 32, 32);
                drainRehash(expires);

                for (int i = 7; i < 10; i++) {
                    expires.removeExpire(keys.get(i));
                }

                Assert.assertFalse(expires.metrics().rehashing());
                Assert.assertTrue(expires.hasMaintenanceDebt());

                publishPendingMaintenance(expires);
                assertResize(expires, 16, 32);
            } finally {
                expires.clear();
                expires.close();
                freeEntries(allocator, entries);
            }
        }
    }

    @Test
    public void stagesPendingShrinkForMaintenanceBeforePublishingTheReplacementTable() {
        try (TestBackend runtime = TestBackend.open("ffm-expire-staged-maintenance");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            List<NativeHandle> entries = new ArrayList<>();
            List<byte[]> keys = new ArrayList<>();
            try {
                for (int i = 0; i < 64; i++) {
                    byte[] key = bytes("staged-expire-" + i);
                    keys.add(key);
                    NativeHandle entry = allocateEntry(allocator, entries);
                    directory.compute(key, (ignored, old) -> new EntryHandle(entry));
                    expires.setExpireAtMillis(directory.getKeyHandle(key), 10_000L + i);
                }
                drainRehash(expires);
                for (int i = 0; i < 60; i++) {
                    var removal = expires.prepareRemoveExpire(directory.getKeyHandle(keys.get(i)));
                    removal.commit();
                    removal.releaseSuperseded();
                }

                HashTableMaintenanceRegistry.Participant participant = expires;
                Assert.assertTrue(participant.hasMaintenanceDebt());
                Assert.assertTrue(participant.estimatedMaintenanceGrowthBytes() > 0L);

                HashTableMaintenanceRegistry.MaintenancePreparation preparation = participant.prepareMaintenance();
                Assert.assertNotNull(preparation);
                Assert.assertFalse(expires.metrics().rehashing());
                Assert.assertTrue(preparation.stagedNonNativeGrowthBytes() > 0L);

                preparation.commit();
                Assert.assertTrue(expires.metrics().rehashing());
                drainRehash(expires);
            } finally {
                expires.clear();
                expires.close();
                freeEntries(allocator, entries);
            }
        }
    }

    @Test
    public void ffmExpireIndexRoundTripsNativeDirectoryHandlesAndByteLookup() {
        try (TestBackend runtime = TestBackend.open("ffm-expire-native-contract");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            byte[] key = bytes("native-key");
            EntryHandle entry = new EntryHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                directory.compute(key, (ignored, old) -> entry);
                KeyHandle handle = directory.getKeyHandle(key);

                Assert.assertNotNull(handle);
                Assert.assertEquals(0, expires.size());

                long expireAt = 123456789L;
                expires.setExpireAtMillis(handle, expireAt);

                Assert.assertEquals(1, expires.size());
                Assert.assertEquals(Long.valueOf(expireAt), expires.get(key));
                Assert.assertEquals(Long.valueOf(expireAt), expires.get(handle));
                Assert.assertNotNull(expires.randomKey());
                Assert.assertNotNull(expires.randomKeyHandle());

                expires.removeExpire(handle);
                Assert.assertNull(expires.get(key));
                Assert.assertEquals(0, expires.size());

                expires.setExpireAtMillis(handle, expireAt + 1);
                Assert.assertEquals(Long.valueOf(expireAt + 1), expires.get(key));
                expires.clear();
                Assert.assertEquals(0, expires.size());
                Assert.assertEquals(16, expires.metrics().capacity());
                Assert.assertEquals(0, expires.metrics().filledSlots());
                Assert.assertEquals(0, expires.metrics().tombstones());
                Assert.assertNull(expires.get(key));
                Assert.assertNull(expires.randomKey());
                Assert.assertNull(expires.randomKeyHandle());
            } finally {
                expires.clear();
                expires.close();
                allocator.free(entry.nativeHandle());
            }
        }
    }

    @Test
    public void ffmExpireIndexDoesNotAllocateIndexOwnedKeyBytes() {
        try (TestBackend runtime = TestBackend.open("ffm-expire-native-key-sharing");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            byte[] key = bytes("shared-key");
            EntryHandle entry = new EntryHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                directory.compute(key, (ignored, old) -> entry);
                KeyHandle handle = directory.getKeyHandle(key);

                Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));

                expires.setExpireAtMillis(handle, 123456789L);
                expires.setExpireAtMillis(handle, 123456790L);

                Assert.assertEquals(1, expires.size());
                Assert.assertEquals(Long.valueOf(123456790L), expires.get(key));
                Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));

                expires.removeExpire(key);
                Assert.assertEquals(0, expires.size());
                Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));
            } finally {
                expires.clear();
                expires.close();
                allocator.free(entry.nativeHandle());
            }
        }
    }

    @Test
    public void removingLastExpiryEntryReleasesEmptyTableRegions() {
        try (TestBackend runtime = TestBackend.open("expire-empty-table-remove");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            byte[] key = bytes("last-expiry-entry");
            EntryHandle entry = new EntryHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                directory.compute(key, (ignored, old) -> entry);
                KeyHandle keyHandle = directory.getKeyHandle(key);
                long baselineLiveRegions = allocator.liveRegionCount();

                expires.setExpireAtMillis(keyHandle, 123456789L);
                Assert.assertEquals(baselineLiveRegions + 3L, allocator.liveRegionCount());

                expires.removeExpire(keyHandle);

                Assert.assertEquals(0, expires.table0Capacity());
                Assert.assertEquals(0, expires.table1Capacity());
                Assert.assertEquals(baselineLiveRegions, allocator.liveRegionCount());
            } finally {
                expires.close();
                allocator.free(entry.nativeHandle());
            }
        }
    }

    @Test
    public void releasingLastPreparedExpiryRemovalReleasesEmptyTableRegions() {
        try (TestBackend runtime = TestBackend.open("expire-empty-table-prepared-remove");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            byte[] key = bytes("last-prepared-expiry-entry");
            EntryHandle entry = new EntryHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                directory.compute(key, (ignored, old) -> entry);
                KeyHandle keyHandle = directory.getKeyHandle(key);
                long baselineLiveRegions = allocator.liveRegionCount();

                expires.setExpireAtMillis(keyHandle, 123456789L);
                var removal = expires.prepareRemoveExpire(keyHandle);
                removal.commit();
                removal.releaseSuperseded();

                Assert.assertEquals(0, expires.table0Capacity());
                Assert.assertEquals(0, expires.table1Capacity());
                Assert.assertEquals(baselineLiveRegions, allocator.liveRegionCount());
            } finally {
                expires.close();
                allocator.free(entry.nativeHandle());
            }
        }
    }

    @Test
    public void expireTableStoresSharedKeyIdentityAsCompleteNativeHandles() throws Exception {
        try (TestBackend runtime = TestBackend.open("expire-complete-key-handles");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, FIXED_SEED, null);
            byte[] key = bytes("complete-key-handle");
            EntryHandle entry = new EntryHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                directory.compute(key, (ignored, old) -> entry);
                KeyHandle keyHandle = directory.getKeyHandle(key);
                NativeHandle expectedHandle = KeyHandleAccess.allocatorNativeHandle(keyHandle);

                expires.setExpireAtMillis(keyHandle, 123456789L);

                Field table0Field = YierdisNativeExpireIndex.class.getDeclaredField("table0");
                table0Field.setAccessible(true);
                Object table = table0Field.get(expires);
                Field handlesField = table.getClass().getDeclaredField("keyHandles");
                handlesField.setAccessible(true);
                Assert.assertEquals(NativeHandle[].class, handlesField.getType());
                NativeHandle[] handles = (NativeHandle[]) handlesField.get(table);

                Assert.assertTrue(Arrays.stream(handles).anyMatch(expectedHandle::equals));
            } finally {
                expires.clear();
                expires.close();
                allocator.free(entry.nativeHandle());
            }
        }
    }

    @Test
    public void expireTableDoesNotMergeEqualLocalRawHandlesFromDifferentBackends() throws Exception {
        try (TestBackend indexRuntime = TestBackend.open("expire-index-collision");
             TestBackend leftRuntime = TestBackend.open("expire-left-collision");
             TestBackend rightRuntime = TestBackend.open("expire-right-collision")) {
            StableMemoryBackend indexBackend = indexRuntime.backend();
            StableMemoryBackend leftBackend = leftRuntime.backend();
            StableMemoryBackend rightBackend = rightRuntime.backend();
            NativeHandle left = leftBackend.allocate(NativeObjectKind.KEY_BYTES, 1);
            NativeHandle right = rightBackend.allocate(NativeObjectKind.KEY_BYTES, 1);
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(indexBackend, FIXED_SEED, null);
            try {
                Assert.assertEquals(left.localRaw(), right.localRaw());
                Assert.assertNotEquals(left.allocatorId(), right.allocatorId());

                Method ensureTable0 = YierdisNativeExpireIndex.class.getDeclaredMethod("ensureTable0");
                ensureTable0.setAccessible(true);
                ensureTable0.invoke(expires);

                Field table0Field = YierdisNativeExpireIndex.class.getDeclaredField("table0");
                table0Field.setAccessible(true);
                Object table = table0Field.get(expires);
                Method insert = YierdisNativeExpireIndex.class.getDeclaredMethod(
                        "insertIntoTable",
                        table.getClass(),
                        NativeHandle.class,
                        int.class,
                        long.class
                );
                insert.setAccessible(true);
                insert.invoke(expires, table, left, 7, 100L);
                insert.invoke(expires, table, right, 7, 200L);

                Field handlesField = table.getClass().getDeclaredField("keyHandles");
                handlesField.setAccessible(true);
                NativeHandle[] handles = (NativeHandle[]) handlesField.get(table);

                Assert.assertEquals(2, expires.size());
                Assert.assertTrue(Arrays.asList(handles).contains(left));
                Assert.assertTrue(Arrays.asList(handles).contains(right));
            } finally {
                expires.close();
                leftBackend.free(left);
                rightBackend.free(right);
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static BytesView view(byte[] value) {
        return new BytesView() {
            @Override
            public int length() {
                return value.length;
            }

            @Override
            public byte getByte(int index) {
                return value[index];
            }
        };
    }

    private static byte[] keyForInitialExpirySlot(int slot) {
        return keyForInitialExpirySlot(slot, 0);
    }

    private static byte[] keyForInitialExpirySlot(int slot, int occurrence) {
        int found = 0;
        for (int candidate = 0; ; candidate++) {
            byte[] key = bytes("expiry-slot-" + candidate);
            if ((expiryHash(key) & 15) != slot) {
                continue;
            }
            if (found++ == occurrence) {
                return key;
            }
        }
    }

    private static int expiryHash(byte[] key) {
        return SipHash24.foldToInt(SipHash24.hash(FIXED_SEED, key));
    }

    private static NativeHandle allocateEntry(StableMemoryBackend allocator, List<NativeHandle> entries) {
        NativeHandle entry = allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32);
        entries.add(entry);
        return entry;
    }

    private static void assertResize(YierdisNativeExpireIndex expires, int capacity, int oldCapacity) {
        HashTableMetrics metrics = expires.metrics();
        Assert.assertTrue(metrics.toString(), metrics.rehashing());
        Assert.assertEquals(metrics.toString(), capacity, metrics.capacity());
        Assert.assertEquals(metrics.toString(), oldCapacity, metrics.oldCapacity());
    }

    private static void drainRehash(YierdisNativeExpireIndex expires) {
        while (expires.metrics().rehashing()) {
            expires.advanceRehash(HashTableWorkBudget.of(64L, Long.MAX_VALUE));
        }
    }

    private static void publishPendingMaintenance(YierdisNativeExpireIndex expires) {
        HashTableMaintenanceRegistry.MaintenancePreparation preparation = expires.prepareMaintenance();
        Assert.assertNotNull(preparation);
        preparation.commit();
    }

    private static void freeEntries(StableMemoryBackend allocator, List<NativeHandle> entries) {
        for (NativeHandle entry : entries) {
            allocator.free(entry);
        }
    }
}
