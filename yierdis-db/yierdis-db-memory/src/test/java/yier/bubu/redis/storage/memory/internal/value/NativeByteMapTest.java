package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class NativeByteMapTest {
    private static final HashSeed FIXED_SEED = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);

    @Test
    public void collisionKeysGrowIntoTwoTablesAndMigrateWithinBudget() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-byte-map-bounded-rehash");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-byte-map-tombstone-reuse");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-byte-map-write-rehash");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
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
    public void putGetReplaceRemoveAndClearReleaseNativeKeys() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-byte-map");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
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
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
        }
    }

    @Test
    public void rehashesAndForEachExposesNativeKeyHandles() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-byte-map-rehash");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
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
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.SET_MEMBER_BYTES));
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
