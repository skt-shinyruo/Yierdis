package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class NativeByteMapTest {
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
