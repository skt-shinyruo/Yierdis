package yier.bubu.redis.storage.memory.internal.expire;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExpireIndexContractTest {
    private static final HashSeed FIXED_SEED = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);

    @Test
    public void registryAdvancesOnlyTheExpiryIndexWithRehashDebtAndUnregistersItWhenComplete() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-registry");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator, FIXED_SEED, registry);
            List<NativeHandle> entries = new ArrayList<>();
            try {
                for (int i = 0; i < 13; i++) {
                    byte[] key = bytes("registry-expire-" + i);
                    NativeHandle entry = allocateEntry(allocator, entries);
                    directory.compute(key, (ignored, old) -> EntryHandle.fromNativeHandle(entry));
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-rehash-budget");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator, FIXED_SEED);
            List<NativeHandle> entries = new ArrayList<>();
            try {
                for (int slot = 4; slot < 16; slot++) {
                    byte[] key = keyForInitialExpirySlot(slot);
                    NativeHandle entry = allocateEntry(allocator, entries);
                    directory.compute(key, (ignored, old) -> EntryHandle.fromNativeHandle(entry));
                    expires.setExpireAtMillis(directory.getKeyHandle(key), 1_000L + slot);
                }
                byte[] growingKey = keyForInitialExpirySlot(4, 1);
                NativeHandle entry = allocateEntry(allocator, entries);
                directory.compute(growingKey, (ignored, old) -> EntryHandle.fromNativeHandle(entry));
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-capacity-policy");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator, FIXED_SEED);
            List<NativeHandle> entries = new ArrayList<>();
            List<byte[]> keys = new ArrayList<>();
            try {
                for (int i = 0; i < 13; i++) {
                    byte[] key = bytes("capacity-policy-" + i);
                    keys.add(key);
                    NativeHandle entry = allocateEntry(allocator, entries);
                    directory.compute(key, (ignored, old) -> EntryHandle.fromNativeHandle(entry));
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-staged-maintenance");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, FIXED_SEED)) {
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator, FIXED_SEED);
            List<NativeHandle> entries = new ArrayList<>();
            List<byte[]> keys = new ArrayList<>();
            try {
                for (int i = 0; i < 64; i++) {
                    byte[] key = bytes("staged-expire-" + i);
                    keys.add(key);
                    NativeHandle entry = allocateEntry(allocator, entries);
                    directory.compute(key, (ignored, old) -> EntryHandle.fromNativeHandle(entry));
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-native-contract");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator);
            byte[] key = bytes("native-key");
            EntryHandle entry = EntryHandle.fromNativeHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-native-key-sharing");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator);
            byte[] key = bytes("shared-key");
            EntryHandle entry = EntryHandle.fromNativeHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
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
    public void ffmExpireTableStoresSharedKeyIdentityInPrimitiveRawHandleArray() throws Exception {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-primitive-key-handles");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator);
            byte[] key = bytes("primitive-key-handle");
            EntryHandle entry = EntryHandle.fromNativeHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                directory.compute(key, (ignored, old) -> entry);
                KeyHandle keyHandle = directory.getKeyHandle(key);
                long expectedRawHandle = KeyHandleAccess.allocatorNativeHandle(keyHandle).raw();

                expires.setExpireAtMillis(keyHandle, 123456789L);

                Field table0Field = YierdisFfmExpireIndex.class.getDeclaredField("table0");
                table0Field.setAccessible(true);
                Object table = table0Field.get(expires);
                Field rawHandlesField = table.getClass().getDeclaredField("keyRawHandles");
                rawHandlesField.setAccessible(true);
                Assert.assertEquals(long[].class, rawHandlesField.getType());
                long[] rawHandles = (long[]) rawHandlesField.get(table);

                Assert.assertTrue(Arrays.stream(rawHandles).anyMatch(raw -> raw == expectedRawHandle));
                Assert.assertFalse(Arrays.stream(table.getClass().getDeclaredFields())
                        .map(Field::getType)
                        .filter(Class::isArray)
                        .anyMatch(type -> !type.getComponentType().isPrimitive()));
                Assert.assertFalse(Arrays.stream(YierdisFfmExpireIndex.class.getDeclaredClasses())
                        .anyMatch(type -> type.getSimpleName().equals("AllocatorKeyRef")));
            } finally {
                expires.clear();
                expires.close();
                allocator.free(entry.nativeHandle());
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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

    private static NativeHandle allocateEntry(NativeAllocator allocator, List<NativeHandle> entries) {
        NativeHandle entry = allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32);
        entries.add(entry);
        return entry;
    }

    private static void assertResize(YierdisFfmExpireIndex expires, int capacity, int oldCapacity) {
        HashTableMetrics metrics = expires.metrics();
        Assert.assertTrue(metrics.toString(), metrics.rehashing());
        Assert.assertEquals(metrics.toString(), capacity, metrics.capacity());
        Assert.assertEquals(metrics.toString(), oldCapacity, metrics.oldCapacity());
    }

    private static void drainRehash(YierdisFfmExpireIndex expires) {
        while (expires.metrics().rehashing()) {
            expires.advanceRehash(HashTableWorkBudget.of(64L, Long.MAX_VALUE));
        }
    }

    private static void publishPendingMaintenance(YierdisFfmExpireIndex expires) {
        HashTableMaintenanceRegistry.MaintenancePreparation preparation = expires.prepareMaintenance();
        Assert.assertNotNull(preparation);
        preparation.commit();
    }

    private static void freeEntries(NativeAllocator allocator, List<NativeHandle> entries) {
        for (NativeHandle entry : entries) {
            allocator.free(entry);
        }
    }
}
