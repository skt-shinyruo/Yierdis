package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.expire.YierdisNativeExpireIndex;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.value.NativeByteMap;
import yier.bubu.redis.storage.memory.internal.value.NativeByteStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HashTableMaintenanceTest {
    @Test
    public void dbMaintenanceUsesTheRegistryAndRespectsTheSlotBudget() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            for (int i = 0; i < 13; i++) {
                Assert.assertTrue(db.writes().strings().setString(
                        bytes("maintenance-" + i),
                        bytes("value"),
                        SetMode.NORMAL,
                        null
                ).value());
            }

            HashTableMaintenanceResult firstTick = db.rehashMaintenance(HashTableWorkBudget.of(3L, Long.MAX_VALUE));

            Assert.assertEquals(3L, firstTick.inspectedSlots());
            Assert.assertEquals(HashTableMaintenanceResult.StopReason.SLOT_LIMIT, firstTick.stopReason());
            Assert.assertEquals(1, firstTick.pendingTableCount());
            HashTableMaintenanceResult current = firstTick;
            while (current.pendingTableCount() != 0) {
                current = db.rehashMaintenance(HashTableWorkBudget.of(4L, Long.MAX_VALUE));
            }
            Assert.assertFalse(db.keyLifecycle().keyDirectory().hasMaintenanceDebt());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void dbMaintenanceStartsPendingShrinkThroughPreparedMutationBeforeMigratingSlots() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            List<byte[]> keys = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                byte[] key = bytes("shrink-" + i);
                keys.add(key);
                Assert.assertTrue(db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null).value());
            }
            drainMaintenance(db);
            int peakCapacity = db.keyLifecycle().keyDirectory().metrics().capacity();

            Assert.assertEquals(Long.valueOf(60L), db.writes().keyspace().del(keys.subList(0, 60)).value());
            Assert.assertTrue(db.keyLifecycle().keyDirectory().hasMaintenanceDebt());

            HashTableMaintenanceResult firstTick = db.rehashMaintenance(HashTableWorkBudget.of(1L, Long.MAX_VALUE));

            Assert.assertEquals(1L, firstTick.inspectedSlots());
            Assert.assertEquals(1, firstTick.pendingTableCount());
            drainMaintenance(db);
            Assert.assertTrue(db.keyLifecycle().keyDirectory().metrics().capacity() < peakCapacity);
            Assert.assertFalse(db.keyLifecycle().keyDirectory().hasMaintenanceDebt());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void memoryStatsExposePendingHashTableWorkAndTheLatestStopReason() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            for (int i = 0; i < 13; i++) {
                Assert.assertTrue(db.writes().strings().setString(
                        bytes("stats-maintenance-" + i),
                        bytes("value"),
                        SetMode.NORMAL,
                        null
                ).value());
            }

            HashTableMaintenanceResult limited = db.rehashMaintenance(HashTableWorkBudget.of(3L, Long.MAX_VALUE));
            YierdisMemoryStats limitedStats = db.memory().memoryStats();

            Assert.assertEquals(limited.pendingTableCount(), limitedStats.pendingHashTableCount());
            Assert.assertEquals(limited.stopReason().name(), limitedStats.lastHashTableMaintenanceStopReason());

            drainMaintenance(db);
            YierdisMemoryStats completeStats = db.memory().memoryStats();
            Assert.assertEquals(0, completeStats.pendingHashTableCount());
            Assert.assertEquals("COMPLETE", completeStats.lastHashTableMaintenanceStopReason());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void capacityRefusalKeepsThePendingTableAuthoritativeAndRegistered() {
        HashSeed seed = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);
        HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
        try (TestBackend runtime = TestBackend.open("hash-table-maintenance-capacity");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteMap<Integer> map = new NativeByteMap<>(
                    new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                    NativeObjectKind.SET_MEMBER_BYTES,
                    seed,
                    registry
            );
            try {
                for (int i = 0; i < 64; i++) {
                    map.put(bytes("capacity-member-" + i), i);
                }
                drainRegistryWithDirectPreparation(registry);

                for (int i = 0; i < 60; i++) {
                    Assert.assertEquals(Integer.valueOf(i), map.remove(bytes("capacity-member-" + i)));
                }

                HashTableMetrics before = map.metrics();
                Assert.assertFalse(before.rehashing());
                Assert.assertTrue(map.hasMaintenanceDebt());
                Assert.assertEquals(1, registry.pendingTableCount());

                HashTableMaintenanceResult refused = registry.advance(
                        HashTableWorkBudget.of(64L, Long.MAX_VALUE),
                        participant -> HashTableMaintenanceRegistry.PreparationResult.CAPACITY_LIMIT
                );

                Assert.assertEquals(HashTableMaintenanceResult.StopReason.CAPACITY_LIMIT, refused.stopReason());
                Assert.assertEquals(0L, refused.inspectedSlots());
                Assert.assertEquals(1, refused.pendingTableCount());
                Assert.assertEquals(before, map.metrics());
                Assert.assertTrue(map.hasMaintenanceDebt());
                Assert.assertEquals(Integer.valueOf(60), map.get(bytes("capacity-member-60")));
                Assert.assertEquals(Integer.valueOf(63), map.get(bytes("capacity-member-63")));
            } finally {
                map.close();
            }
        }
    }

    @Test
    public void registrySkipsTenThousandIdleMapsAndFairlyAdvancesFiveDebtParticipants() {
        HashSeed seed = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);
        HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
        try (TestBackend runtime = TestBackend.open("hash-table-maintenance-scale");
             StableMemoryBackend allocator = runtime.backend();
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator, seed, registry)) {
            YierdisNativeExpireIndex expires = new YierdisNativeExpireIndex(allocator, seed, registry);
            NativeByteMap<Integer> hashMembers = new NativeByteMap<>(
                    new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES),
                    NativeObjectKind.HASH_FIELD_BYTES,
                    seed,
                    registry
            );
            NativeByteMap<Integer> setMembers = new NativeByteMap<>(
                    new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                    NativeObjectKind.SET_MEMBER_BYTES,
                    seed,
                    registry
            );
            NativeByteMap<Integer> zsetMembers = new NativeByteMap<>(
                    new NativeByteStore(allocator, NativeObjectKind.ZSET_MEMBER_BYTES),
                    NativeObjectKind.ZSET_MEMBER_BYTES,
                    seed,
                    registry
            );
            List<NativeByteMap<Integer>> idleMaps = new ArrayList<>(10_000);
            List<NativeHandle> entries = new ArrayList<>();
            try {
                NativeByteStore idleStore = new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES);
                for (int i = 0; i < 10_000; i++) {
                    idleMaps.add(new NativeByteMap<>(idleStore, NativeObjectKind.SET_MEMBER_BYTES, seed, registry));
                }
                long idleRegistryHeapBytes = registry.heapEstimatedBytes();

                for (int i = 0; i < 13; i++) {
                    byte[] key = bytes("maintenance-key-" + i);
                    NativeHandle entry = allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32);
                    entries.add(entry);
                    directory.compute(key, (ignored, old) -> new EntryHandle(entry));
                    expires.setExpireAtMillis(directory.getKeyHandle(key), 10_000L + i);
                    hashMembers.put(bytes("hash-member-" + i), i);
                    setMembers.put(bytes("set-member-" + i), i);
                    zsetMembers.put(bytes("zset-member-" + i), i);
                }

                Assert.assertEquals(5, registry.pendingTableCount());
                Assert.assertEquals(idleRegistryHeapBytes + 5L * 40L, registry.heapEstimatedBytes());
                for (NativeByteMap<Integer> idleMap : idleMaps) {
                    Assert.assertFalse(idleMap.hasMaintenanceDebt());
                }
                assertNativeByteMapHeapFormula(hashMembers);
                assertNativeByteMapHeapFormula(setMembers);
                assertNativeByteMapHeapFormula(zsetMembers);

                HashTableMaintenanceResult firstTick = registry.advance(HashTableWorkBudget.of(12L, Long.MAX_VALUE));

                Assert.assertTrue(firstTick.inspectedSlots() <= 12L);
                Assert.assertEquals(HashTableMaintenanceResult.StopReason.SLOT_LIMIT, firstTick.stopReason());
                Assert.assertEquals(5, firstTick.pendingTableCount());
                Assert.assertTrue(directory.hasMaintenanceDebt());
                Assert.assertTrue(expires.hasMaintenanceDebt());
                Assert.assertTrue(hashMembers.hasMaintenanceDebt());
                Assert.assertTrue(setMembers.hasMaintenanceDebt());
                Assert.assertTrue(zsetMembers.hasMaintenanceDebt());

                HashTableMaintenanceResult result = firstTick;
                for (int i = 0; result.pendingTableCount() != 0 && i < 1_000; i++) {
                    result = registry.advance(HashTableWorkBudget.of(12L, Long.MAX_VALUE));
                }

                Assert.assertEquals(0, result.pendingTableCount());
                Assert.assertEquals(HashTableMaintenanceResult.StopReason.COMPLETE, result.stopReason());
                Assert.assertEquals(idleRegistryHeapBytes, registry.heapEstimatedBytes());
                assertNativeByteMapHeapFormula(hashMembers);
                assertNativeByteMapHeapFormula(setMembers);
                assertNativeByteMapHeapFormula(zsetMembers);

                NativeByteMap<Integer> closable = new NativeByteMap<>(
                        new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                        NativeObjectKind.SET_MEMBER_BYTES,
                        seed,
                        registry
                );
                try {
                    for (int i = 0; i < 13; i++) {
                        closable.put(bytes("close-member-" + i), i);
                    }
                    Assert.assertEquals(1, registry.pendingTableCount());

                    closable.close();

                    Assert.assertEquals(0, registry.pendingTableCount());
                    Assert.assertEquals(idleRegistryHeapBytes, registry.heapEstimatedBytes());
                } finally {
                    closable.close();
                }
            } finally {
                expires.close();
                hashMembers.close();
                setMembers.close();
                zsetMembers.close();
                for (NativeHandle entry : entries) {
                    allocator.free(entry);
                }
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void drainMaintenance(YierdisDb db) {
        HashTableMaintenanceResult result = db.rehashMaintenance(HashTableWorkBudget.of(64L, Long.MAX_VALUE));
        for (int i = 0; result.pendingTableCount() != 0 && i < 1_000; i++) {
            result = db.rehashMaintenance(HashTableWorkBudget.of(64L, Long.MAX_VALUE));
        }
        Assert.assertEquals(0, result.pendingTableCount());
    }

    private static void drainRegistryWithDirectPreparation(HashTableMaintenanceRegistry registry) {
        HashTableMaintenanceResult result = registry.advance(
                HashTableWorkBudget.of(64L, Long.MAX_VALUE),
                participant -> {
                    HashTableMaintenanceRegistry.MaintenancePreparation preparation = participant.prepareMaintenance();
                    if (preparation == null) {
                        return HashTableMaintenanceRegistry.PreparationResult.NO_CHANGE;
                    }
                    boolean committed = false;
                    try {
                        preparation.commit();
                        committed = true;
                        return HashTableMaintenanceRegistry.PreparationResult.STARTED;
                    } finally {
                        if (!committed) {
                            preparation.abort();
                        }
                    }
                }
        );
        for (int i = 0; result.pendingTableCount() != 0 && i < 1_000; i++) {
            result = registry.advance(
                    HashTableWorkBudget.of(64L, Long.MAX_VALUE),
                    participant -> {
                        HashTableMaintenanceRegistry.MaintenancePreparation preparation = participant.prepareMaintenance();
                        if (preparation == null) {
                            return HashTableMaintenanceRegistry.PreparationResult.NO_CHANGE;
                        }
                        boolean committed = false;
                        try {
                            preparation.commit();
                            committed = true;
                            return HashTableMaintenanceRegistry.PreparationResult.STARTED;
                        } finally {
                            if (!committed) {
                                preparation.abort();
                            }
                        }
                    }
            );
        }
        Assert.assertEquals(0, result.pendingTableCount());
    }

    private static void assertNativeByteMapHeapFormula(NativeByteMap<?> map) {
        var metrics = map.metrics();
        long expected = nativeByteMapTableHeapBytes(metrics.capacity())
                + (metrics.rehashing() ? nativeByteMapTableHeapBytes(metrics.oldCapacity()) : 0L);
        Assert.assertEquals(expected, map.heapEstimatedBytes());
    }

    private static long nativeByteMapTableHeapBytes(int capacity) {
        return 48L
                + (16L + (long) capacity * Long.BYTES) * 2L
                + 16L + (long) capacity * Integer.BYTES
                + 16L + capacity;
    }
}
