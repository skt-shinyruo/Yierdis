package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NativeByteMapTest {
    private static final HashSeed FIXED_SEED = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);

    @Test
    public void nativeHandleValuesPreserveBothAllocatorIdsAcrossLocalRawCollisions() {
        TestOwner leftOwner = new TestOwner();
        TestOwner rightOwner = new TestOwner();
        HeapStableMemoryBackend leftBackend = new HeapStableMemoryBackend("map-left", 16, leftOwner);
        HeapStableMemoryBackend rightBackend = new HeapStableMemoryBackend("map-right", 16, rightOwner);
        leftBackend.bindToCurrentThread();
        rightBackend.bindToCurrentThread();

        NativeHandle leftValue = leftBackend.allocate(NativeObjectKind.HASH_VALUE_BYTES, 1);
        NativeHandle rightValue = rightBackend.allocate(NativeObjectKind.HASH_VALUE_BYTES, 1);
        Assert.assertEquals(leftValue.localRaw(), rightValue.localRaw());
        Assert.assertNotEquals(leftValue.allocatorId(), rightValue.allocatorId());

        NativeByteStore keyStore = new NativeByteStore(leftBackend, NativeObjectKind.HASH_FIELD_BYTES);
        try (NativeByteMap<NativeHandle> map = NativeByteMap.nativeHandleValues(
                keyStore,
                NativeObjectKind.HASH_FIELD_BYTES,
                FIXED_SEED,
                null,
                null
        )) {
            map.put(bytes("left"), leftValue);
            map.put(bytes("right"), rightValue);

            Assert.assertEquals(leftValue, map.get(bytes("left")));
            Assert.assertEquals(rightValue, map.get(bytes("right")));
        }

        leftBackend.free(leftValue);
        rightBackend.free(rightValue);
        leftBackend.close();
        rightBackend.close();
    }

    @Test
    public void tableStoresKeysAsCompleteNativeHandles() throws ReflectiveOperationException {
        Class<?> tableClass = Class.forName(NativeByteMap.class.getName() + "$Table");

        var keyHandles = tableClass.getDeclaredField("keyHandles");
        Assert.assertEquals(NativeHandle[].class, keyHandles.getType());
    }

    @Test
    public void specializedValueLayoutsUsePrimitiveOrNoValueArray() throws ReflectiveOperationException {
        try (TestBackend runtime = TestBackend.open("native-byte-map-value-layouts");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore keyStore = new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES);
            NativeByteStore valueStore = new NativeByteStore(allocator, NativeObjectKind.HASH_VALUE_BYTES);
            Object present = new Object();
            try (NativeByteMap<NativeHandle> handles = NativeByteMap.nativeHandleValues(
                    keyStore,
                    NativeObjectKind.HASH_FIELD_BYTES,
                    FIXED_SEED,
                    null,
                    null
            ); NativeByteMap<Object> constants = NativeByteMap.constantValues(
                    keyStore,
                    NativeObjectKind.HASH_FIELD_BYTES,
                    FIXED_SEED,
                    null,
                    null,
                    present
            ); NativeByteMap<Integer> objects = new NativeByteMap<>(
                    keyStore,
                    NativeObjectKind.HASH_FIELD_BYTES,
                    FIXED_SEED
            )) {
                NativeHandle value = valueStore.store(bytes("value"));
                try {
                    Assert.assertNull(handles.put(bytes("handle"), value));
                    Assert.assertEquals(value, handles.get(bytes("handle")));
                    Assert.assertNull(constants.put(bytes("constant"), present));
                    Assert.assertSame(present, constants.put(bytes("constant"), present));
                    objects.put(bytes("object"), 1);

                    Assert.assertTrue(activeValueSlots(handles) instanceof NativeHandle[]);
                    Assert.assertNull(activeValueSlots(constants));
                    Assert.assertTrue(activeValueSlots(objects) instanceof Object[]);

                    int capacity = handles.metrics().capacity();
                    Assert.assertEquals(tableHeapBytes(capacity, true), handles.heapEstimatedBytes());
                    Assert.assertEquals(tableHeapBytes(capacity, false), constants.heapEstimatedBytes());
                    Assert.assertEquals(tableHeapBytes(capacity, true), objects.heapEstimatedBytes());
                } finally {
                    valueStore.release(value);
                }
            }
        }
    }

    @Test
    public void registryAdvancesOnlyTheMapWithRehashDebtAndUnregistersItWhenComplete() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-registry");
             StableMemoryBackend allocator = runtime.backend()) {
            HashTableMaintenanceRegistry registry = new HashTableMaintenanceRegistry();
            try (NativeByteMap<Integer> map = new NativeByteMap<>(
                    new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                    NativeObjectKind.SET_MEMBER_BYTES,
                    FIXED_SEED,
                    registry
            )) {
                for (int i = 0; i < 13; i++) {
                    map.put(bytes("registry-" + i), i);
                }

                Assert.assertTrue(map.metrics().rehashing());
                Assert.assertEquals(1, registry.pendingTableCount());

                HashTableMaintenanceResult firstTick = registry.advance(HashTableWorkBudget.of(3L, Long.MAX_VALUE));

                Assert.assertEquals(3L, firstTick.inspectedSlots());
                Assert.assertEquals(HashTableMaintenanceResult.StopReason.SLOT_LIMIT, firstTick.stopReason());
                while (registry.pendingTableCount() != 0) {
                    registry.advance(HashTableWorkBudget.of(4L, Long.MAX_VALUE));
                }
                Assert.assertFalse(map.hasMaintenanceDebt());
                Assert.assertFalse(map.metrics().rehashing());
            }
        }
    }

    @Test
    public void collisionKeysGrowIntoTwoTablesAndMigrateWithinBudget() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-bounded-rehash");
             StableMemoryBackend allocator = runtime.backend();
             NativeByteMap<Integer> map = new NativeByteMap<>(
                     new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                     NativeObjectKind.SET_MEMBER_BYTES,
                     FIXED_SEED,
                     ignored -> 7
             )) {
            for (int i = 0; i < 13; i++) {
                Assert.assertNull(map.put(bytes("collision-" + i), i));
            }

            Assert.assertTrue(map.metrics().rehashing());
            HashTableWorkResult step = map.advanceRehash(HashTableWorkBudget.of(3L, Long.MAX_VALUE));
            Assert.assertEquals(3L, step.inspectedSlots());
            Assert.assertTrue(step.migratedSlots() <= 3L);
            for (int i = 0; i < 13; i++) {
                Assert.assertEquals(Integer.valueOf(i), map.get(bytes("collision-" + i)));
            }
        }
    }

    @Test
    public void reusingATombstoneDoesNotStartAnUnnecessaryGrow() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-tombstone-reuse");
             StableMemoryBackend allocator = runtime.backend();
             NativeByteMap<Integer> map = new NativeByteMap<>(
                     new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                     NativeObjectKind.SET_MEMBER_BYTES,
                     FIXED_SEED,
                     ignored -> 7
             )) {
            for (int i = 0; i < 12; i++) {
                map.put(bytes("collision-" + i), i);
            }

            Assert.assertEquals(Integer.valueOf(0), map.remove(bytes("collision-0")));
            Assert.assertNull(map.put(bytes("replacement"), 12));

            Assert.assertFalse(map.metrics().rehashing());
            Assert.assertEquals(12, map.metrics().filledSlots());
            Assert.assertEquals(0, map.metrics().tombstones());
        }
    }

    @Test
    public void everyWriteAdvancesAnActiveRehash() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-write-rehash");
             StableMemoryBackend allocator = runtime.backend();
             NativeByteMap<Integer> map = new NativeByteMap<>(
                     new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                     NativeObjectKind.SET_MEMBER_BYTES,
                     FIXED_SEED,
                     ignored -> 7
             )) {
            for (int i = 0; i < 13; i++) {
                map.put(bytes("collision-" + i), i);
            }
            Assert.assertTrue(map.metrics().rehashing());
            Assert.assertEquals(0, map.metrics().rehashCursor());

            Assert.assertEquals(Integer.valueOf(0), map.replace(bytes("collision-0"), 99));

            Assert.assertTrue(map.metrics().rehashCursor() >= 2);
            Assert.assertEquals(Integer.valueOf(99), map.get(bytes("collision-0")));
        }
    }

    @Test
    public void collidingKeysCompactAndShrinkThroughBoundedMaintenance() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-maintenance");
             StableMemoryBackend allocator = runtime.backend();
             NativeByteMap<Integer> map = new NativeByteMap<>(
                     new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                     NativeObjectKind.SET_MEMBER_BYTES,
                     FIXED_SEED,
                     ignored -> 7
             )) {
            for (int i = 0; i < 512; i++) {
                Assert.assertNull(map.put(bytes("collision-" + i), i));
            }
            drainRehash(map);
            int peakCapacity = map.metrics().capacity();

            Assert.assertEquals(Integer.valueOf(17), map.put(bytes("collision-17"), 9_999));
            Assert.assertEquals(512, map.size());

            for (int i = 0; i < 480; i++) {
                Assert.assertEquals(Integer.valueOf(i == 17 ? 9_999 : i), map.remove(bytes("collision-" + i)));
            }

            Assert.assertTrue(map.hasMaintenanceDebt());
            while (map.hasMaintenanceDebt()) {
                try (NativeByteMap<Integer>.StagedResize staged = map.stageMaintenanceResize()) {
                    Assert.assertNotNull(staged);
                    map.publishStagedResize(staged);
                }
                drainRehash(map);
            }

            Assert.assertTrue(map.metrics().capacity() < peakCapacity);
            Assert.assertTrue(map.metrics().tombstones() <= Math.max(
                    map.metrics().size(),
                    map.metrics().capacity() / 8
            ));
            Assert.assertEquals(32, map.size());
            for (int i = 0; i < 480; i++) {
                Assert.assertNull(map.get(bytes("collision-" + i)));
            }
            for (int i = 480; i < 512; i++) {
                Assert.assertEquals(Integer.valueOf(i), map.get(bytes("collision-" + i)));
            }

            map.clear();
            Assert.assertEquals(16, map.metrics().capacity());
            Assert.assertEquals(0, map.metrics().size());
            Assert.assertEquals(0, map.metrics().filledSlots());
            Assert.assertEquals(0, map.metrics().tombstones());
        }
    }

    @Test
    public void stagesPendingShrinkForMaintenanceBeforePublishingTheReplacementTable() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-staged-maintenance");
             StableMemoryBackend allocator = runtime.backend();
             NativeByteMap<Integer> map = new NativeByteMap<>(
                     new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES),
                     NativeObjectKind.SET_MEMBER_BYTES,
                     FIXED_SEED
             )) {
            for (int i = 0; i < 64; i++) {
                map.put(bytes("staged-" + i), i);
            }
            drainRehash(map);
            for (int i = 0; i < 60; i++) {
                Assert.assertEquals(Integer.valueOf(i), map.remove(bytes("staged-" + i)));
            }

            HashTableMaintenanceRegistry.Participant participant = map;
            Assert.assertTrue(participant.hasMaintenanceDebt());
            Assert.assertTrue(participant.estimatedMaintenanceGrowthBytes() > 0L);

            HashTableMaintenanceRegistry.MaintenancePreparation preparation = participant.prepareMaintenance();
            Assert.assertNotNull(preparation);
            Assert.assertFalse(map.metrics().rehashing());
            Assert.assertTrue(preparation.stagedNonNativeGrowthBytes() > 0L);

            preparation.commit();
            Assert.assertTrue(map.metrics().rehashing());
            drainRehash(map);
        }
    }

    @Test
    public void putGetReplaceRemoveAndClearReleaseNativeKeys() {
        try (TestBackend runtime = TestBackend.open("native-byte-map");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES);
            NativeByteMap<String> map = new NativeByteMap<>(store, NativeObjectKind.HASH_FIELD_BYTES);

            Assert.assertNull(map.put(bytes("a"), "one"));
            Assert.assertNull(map.put(bytes("b"), "two"));
            long nativeBytes = map.nativeBytes();
            Assert.assertTrue(nativeBytes >= 2L);
            Assert.assertEquals("one", map.get(bytes("a")));

            Assert.assertEquals("one", map.put(bytes("a"), "next"));
            Assert.assertEquals(nativeBytes, map.nativeBytes());
            Assert.assertEquals("next", map.get(bytes("a")));

            Assert.assertEquals("two", map.remove(bytes("b")));
            Assert.assertNull(map.get(bytes("b")));

            map.clear();
            Assert.assertNull(map.get(bytes("a")));
            Assert.assertEquals(0L, map.nativeBytes());
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    @Test
    public void preparedUpdateAndAddRemainInvisibleAndAbortReleasesOnlyTheStagedKey() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-prepared-abort");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES);
            try (NativeByteMap<String> map = new NativeByteMap<>(
                    store,
                    NativeObjectKind.HASH_FIELD_BYTES,
                    FIXED_SEED
            )) {
                map.put(bytes("existing"), "before");
                NativeHandle existingHandle = keyHandle(map, store, "existing");
                long ownedStoreBytes = store.nativeBytes();
                long ownedMapBytes = map.nativeBytes();
                try (NativeByteMap.PreparedMutation<String> prepared = map.preparePuts(List.of(
                        new NativeByteMap.StagedPut<>(bytes("existing"), "after", true),
                        new NativeByteMap.StagedPut<>(bytes("added"), "new", false)
                ))) {
                    Assert.assertEquals(1, prepared.addedCount());
                    Assert.assertEquals("before", prepared.previousValue(0));
                    Assert.assertNull(prepared.previousValue(1));
                    Assert.assertEquals("before", map.get(bytes("existing")));
                    Assert.assertNull(map.get(bytes("added")));
                    Assert.assertEquals(1, map.size());
                    Assert.assertEquals(ownedMapBytes, map.nativeBytes());
                    Assert.assertTrue(store.nativeBytes() > ownedStoreBytes);
                }

                Assert.assertEquals("before", map.get(bytes("existing")));
                Assert.assertNull(map.get(bytes("added")));
                Assert.assertEquals(1, map.size());
                Assert.assertEquals(existingHandle.localRaw(), keyHandle(map, store, "existing").localRaw());
                Assert.assertEquals(ownedMapBytes, map.nativeBytes());
                Assert.assertEquals(ownedStoreBytes, store.nativeBytes());
            }
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    @Test
    public void preparedCommitPreservesExistingRawHandleAcrossTopologyReplacement() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-prepared-commit");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES);
            try (NativeByteMap<String> map = new NativeByteMap<>(
                    store,
                    NativeObjectKind.HASH_FIELD_BYTES,
                    FIXED_SEED
            )) {
                for (int i = 0; i < 12; i++) {
                    map.put(bytes("field-" + i), "value-" + i);
                }
                NativeHandle existingHandle = keyHandle(map, store, "field-0");
                int sourceCapacity = map.metrics().capacity();

                try (NativeByteMap.PreparedMutation<String> prepared = map.preparePuts(List.of(
                        new NativeByteMap.StagedPut<>(bytes("field-0"), "updated", true),
                        new NativeByteMap.StagedPut<>(bytes("field-12"), "value-12", false)
                ))) {
                    Assert.assertEquals("value-0", map.get(bytes("field-0")));
                    Assert.assertNull(map.get(bytes("field-12")));
                    prepared.commit();
                    prepared.releaseSuperseded();
                }

                NativeHandle committedHandle = keyHandle(map, store, "field-0");
                Assert.assertTrue(map.metrics().capacity() > sourceCapacity);
                Assert.assertEquals(existingHandle.localRaw(), committedHandle.localRaw());
                Assert.assertArrayEquals(bytes("field-0"), store.toByteArray(committedHandle));
                Assert.assertEquals("updated", map.get(bytes("field-0")));
                Assert.assertEquals("value-12", map.get(bytes("field-12")));
                Assert.assertEquals(13, map.size());
            }
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    @Test
    public void preparedTopologyReplacementRejectsAnUnrelatedValueChange() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-prepared-stale");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES);
            try (NativeByteMap<String> map = new NativeByteMap<>(
                    store,
                    NativeObjectKind.HASH_FIELD_BYTES,
                    FIXED_SEED
            )) {
                for (int index = 0; index < 12; index++) {
                    map.put(bytes("field-" + index), "value-" + index);
                }
                long nativeBytesBefore = map.nativeBytes();

                try (NativeByteMap.PreparedMutation<String> prepared = map.preparePuts(List.of(
                        new NativeByteMap.StagedPut<>(bytes("field-12"), "value-12", false)
                ))) {
                    Assert.assertEquals("value-1", map.replace(bytes("field-1"), "new-value-1"));
                    Assert.assertThrows(IllegalStateException.class, prepared::commit);
                }

                Assert.assertEquals(12, map.size());
                Assert.assertEquals("new-value-1", map.get(bytes("field-1")));
                Assert.assertNull(map.get(bytes("field-12")));
                Assert.assertEquals(nativeBytesBefore, map.nativeBytes());
            }
        }
    }

    @Test
    public void borrowedMapNeverReleasesExternalKeysOnRemoveClearOrClose() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-borrowed-keys");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.ZSET_MEMBER_BYTES);
            NativeHandle removedKey = store.store(bytes("removed"));
            NativeHandle clearedKey = store.store(bytes("cleared"));
            NativeHandle closedKey = store.store(bytes("closed"));
            long externalBytes = store.nativeBytes();

            try (NativeByteMap<Integer> map = NativeByteMap.borrowedKeys(
                    store,
                    NativeObjectKind.ZSET_MEMBER_BYTES,
                    FIXED_SEED,
                    null,
                    null
            )) {
                Assert.assertNull(map.putBorrowed(removedKey, 1));
                Assert.assertEquals(Integer.valueOf(1), map.remove(removedKey));
                Assert.assertArrayEquals(bytes("removed"), store.toByteArray(removedKey));

                Assert.assertNull(map.putBorrowed(clearedKey, 2));
                map.clear();
                Assert.assertArrayEquals(bytes("cleared"), store.toByteArray(clearedKey));

                Assert.assertNull(map.putBorrowed(closedKey, 3));
                Assert.assertEquals(0L, map.nativeBytes());
                Assert.assertEquals(externalBytes, store.nativeBytes());
            }

            Assert.assertArrayEquals(bytes("removed"), store.toByteArray(removedKey));
            Assert.assertArrayEquals(bytes("cleared"), store.toByteArray(clearedKey));
            Assert.assertArrayEquals(bytes("closed"), store.toByteArray(closedKey));
            Assert.assertEquals(externalBytes, store.nativeBytes());
            store.release(removedKey);
            store.release(clearedKey);
            store.release(closedKey);
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    @Test
    public void rehashesAndForEachExposesNativeKeyHandles() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-rehash");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES);
            NativeByteMap<Integer> map = new NativeByteMap<>(store, NativeObjectKind.SET_MEMBER_BYTES);

            for (int i = 0; i < 40; i++) {
                Assert.assertNull(map.put(bytes("k" + i), i));
            }

            for (int i = 0; i < 40; i++) {
                Assert.assertEquals(Integer.valueOf(i), map.get(bytes("k" + i)));
            }

            List<String> keys = new ArrayList<>();
            map.forEach((keyHandle, value) -> keys.add(new String(store.toByteArray(keyHandle), StandardCharsets.US_ASCII)));
            Assert.assertEquals(40, keys.size());
            Assert.assertTrue(keys.contains("k0"));
            Assert.assertTrue(keys.contains("k39"));

            map.clear();
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    @Test
    public void scanVisitsEveryEntryAcrossBoundedWindows() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-scan");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES);
            try (NativeByteMap<Integer> map = new NativeByteMap<>(
                    store,
                    NativeObjectKind.SET_MEMBER_BYTES,
                    FIXED_SEED
            )) {
                for (int i = 0; i < 40; i++) {
                    map.put(bytes("scan-" + i), i);
                }
                drainRehash(map);

                Set<String> seen = new HashSet<>();
                ScanCursorV2 cursor = ScanCursorV2.start();
                do {
                    NativeByteMap.ScanResult result = map.scanWithWork(cursor, 3L, (keyHandle, value) -> {
                        String key = new String(store.toByteArray(keyHandle), StandardCharsets.US_ASCII);
                        seen.add(key);
                        Assert.assertEquals(Integer.valueOf(Integer.parseInt(key.substring("scan-".length()))), value);
                        return true;
                    });
                    Assert.assertTrue(result.inspectedSlots() <= 3L);
                    cursor = result.nextCursor();
                } while (cursor.value() != 0L);

                Assert.assertEquals(40, seen.size());
                for (int i = 0; i < 40; i++) {
                    Assert.assertTrue(seen.contains("scan-" + i));
                }
            }
        }
    }

    @Test
    public void scanUsesMigratedOldSlotsAndRestartsAfterGenerationChange() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-scan-rehash");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES);
            try (NativeByteMap<Integer> map = new NativeByteMap<>(
                    store,
                    NativeObjectKind.SET_MEMBER_BYTES,
                    FIXED_SEED,
                    ignored -> 7
            )) {
                for (int i = 0; i < 13; i++) {
                    map.put(bytes("collision-" + i), i);
                }

                Assert.assertTrue(map.metrics().rehashing());
                long generation = map.metrics().generation();
                ScanCursorV2 oldPhase = map.scan(
                        ScanCursorV2.start(),
                        map.metrics().capacity(),
                        (keyHandle, value) -> true
                );
                Assert.assertEquals((int) generation, oldPhase.generation());
                Assert.assertEquals(1, oldPhase.phase());
                Assert.assertEquals(0L, oldPhase.position());

                map.advanceRehash(HashTableWorkBudget.of(8L, Long.MAX_VALUE));
                Set<String> migratedFromOldPhase = new HashSet<>();
                ScanCursorV2 afterOldPrefix = map.scan(oldPhase, 8, (keyHandle, value) -> {
                    migratedFromOldPhase.add(new String(store.toByteArray(keyHandle), StandardCharsets.US_ASCII));
                    return true;
                });

                Assert.assertTrue(migratedFromOldPhase.contains("collision-0"));
                Assert.assertEquals(1, afterOldPrefix.phase());
                Assert.assertEquals(8L, afterOldPrefix.position());

                Set<String> seenWhileMigrating = new HashSet<>();
                ScanCursorV2 interleavedCursor = ScanCursorV2.start();
                int calls = 0;
                do {
                    NativeByteMap.ScanResult result = map.scanWithWork(
                            interleavedCursor,
                            2L,
                            (keyHandle, value) -> {
                                seenWhileMigrating.add(new String(
                                        store.toByteArray(keyHandle),
                                        StandardCharsets.US_ASCII
                                ));
                                return true;
                            }
                    );
                    interleavedCursor = result.nextCursor();
                    if (map.metrics().rehashing()) {
                        map.advanceRehash(HashTableWorkBudget.of(1L, Long.MAX_VALUE));
                    }
                    calls++;
                    Assert.assertTrue("interleaved scan must terminate", calls < 128);
                } while (interleavedCursor.value() != 0L);

                Assert.assertFalse(map.metrics().rehashing());
                for (int i = 0; i < 13; i++) {
                    Assert.assertTrue(seenWhileMigrating.contains("collision-" + i));
                }

                Set<String> restarted = new HashSet<>();
                ScanCursorV2 complete = map.scan(oldPhase, map.metrics().capacity(), (keyHandle, value) -> {
                    restarted.add(new String(store.toByteArray(keyHandle), StandardCharsets.US_ASCII));
                    return true;
                });

                Assert.assertEquals(0L, complete.value());
                for (int i = 0; i < 13; i++) {
                    Assert.assertTrue(restarted.contains("collision-" + i));
                }
            }
        }
    }

    @Test
    public void scanResolvesMigratedShadowValueFromTheActiveTable() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-scan-shadow-value");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore keyStore = new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES);
            NativeByteStore valueStore = new NativeByteStore(allocator, NativeObjectKind.HASH_VALUE_BYTES);
            try (NativeByteMap<NativeHandle> map = NativeByteMap.nativeHandleValues(
                    keyStore,
                    NativeObjectKind.HASH_FIELD_BYTES,
                    FIXED_SEED,
                    ignored -> 7
            )) {
                try {
                    for (int i = 0; i < 13; i++) {
                        map.put(bytes("field-" + i), valueStore.store(bytes("value-" + i)));
                    }
                    Assert.assertTrue(map.metrics().rehashing());

                    ScanCursorV2 oldPhase = map.scan(
                            ScanCursorV2.start(),
                            map.metrics().capacity(),
                            (keyHandle, valueHandle) -> true
                    );
                    map.advanceRehash(HashTableWorkBudget.of(8L, Long.MAX_VALUE));

                    NativeHandle replacement = valueStore.store(bytes("replacement"));
                    NativeHandle released = map.replace(bytes("field-0"), replacement);
                    Assert.assertNotNull(released);
                    valueStore.release(released);

                    Set<String> scanned = new HashSet<>();
                    ScanCursorV2 next = map.scan(oldPhase, 8, (keyHandle, valueHandle) -> {
                        String field = new String(keyStore.toByteArray(keyHandle), StandardCharsets.US_ASCII);
                        scanned.add(field);
                        if (field.equals("field-0")) {
                            Assert.assertEquals(replacement, valueHandle);
                            Assert.assertArrayEquals(bytes("replacement"), valueStore.toByteArray(valueHandle));
                        }
                        return true;
                    });

                    Assert.assertTrue(scanned.contains("field-0"));
                    Assert.assertEquals(1, next.phase());
                    Assert.assertEquals(8L, next.position());
                } finally {
                    map.forEach((keyHandle, valueHandle) -> valueStore.release(valueHandle));
                }
            }
            Assert.assertEquals(0L, valueStore.nativeBytes());
        }
    }

    @Test
    public void scanCanStopAfterTheFirstVisibleEntry() {
        try (TestBackend runtime = TestBackend.open("native-byte-map-scan-stop");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES);
            try (NativeByteMap<Integer> map = new NativeByteMap<>(store, NativeObjectKind.HASH_FIELD_BYTES)) {
                map.put(bytes("one"), 1);
                map.put(bytes("two"), 2);

                int[] visited = {0};
                NativeByteMap.ScanResult result = map.scanWithWork(
                        ScanCursorV2.start(),
                        map.metrics().capacity(),
                        (keyHandle, value) -> {
                            visited[0]++;
                            return false;
                        }
                );

                Assert.assertEquals(1, visited[0]);
                Assert.assertTrue(result.inspectedSlots() > 0L);
                Assert.assertTrue(result.nextCursor().value() > 0L);
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> map.scanWithWork(ScanCursorV2.start(), -1L, (keyHandle, value) -> true)
                );
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static void drainRehash(NativeByteMap<?> map) {
        while (map.metrics().rehashing()) {
            map.advanceRehash(HashTableWorkBudget.of(64L, Long.MAX_VALUE));
        }
    }

    private static NativeHandle keyHandle(NativeByteMap<?> map, NativeByteStore store, String expectedKey) {
        NativeHandle[] found = {null};
        map.forEach((keyHandle, value) -> {
            String key = new String(store.toByteArray(keyHandle), StandardCharsets.US_ASCII);
            if (expectedKey.equals(key)) {
                Assert.assertNull(found[0]);
                found[0] = keyHandle;
            }
        });
        Assert.assertNotNull(found[0]);
        return found[0];
    }

    private static Object activeValueSlots(NativeByteMap<?> map) throws ReflectiveOperationException {
        var activeField = NativeByteMap.class.getDeclaredField("active");
        activeField.setAccessible(true);
        Object table = activeField.get(map);
        var valueSlotsField = table.getClass().getDeclaredField("valueSlots");
        valueSlotsField.setAccessible(true);
        return valueSlotsField.get(table);
    }

    private static long tableHeapBytes(int capacity, boolean hasValueArray) {
        long valueArrayBytes = hasValueArray ? 16L + (long) capacity * Long.BYTES : 0L;
        return 48L
                + 16L + (long) capacity * Long.BYTES
                + valueArrayBytes
                + 16L + (long) capacity * Integer.BYTES
                + 16L + capacity;
    }

    private static final class TestOwner implements MemoryOwner {
        private Thread owner;

        @Override
        public void bindToCurrentThread() {
            Thread current = Thread.currentThread();
            if (owner == null) {
                owner = current;
                return;
            }
            if (owner != current) {
                throw new IllegalStateException("owner is bound to another thread");
            }
        }

        @Override
        public void checkCurrentThread() {
            if (owner != Thread.currentThread()) {
                throw new IllegalStateException("access is outside the owner thread");
            }
        }

        @Override
        public void checkCurrentThreadForShutdown() {
            if (owner != null && owner != Thread.currentThread()) {
                throw new IllegalStateException("shutdown is outside the owner thread");
            }
        }
    }
}
