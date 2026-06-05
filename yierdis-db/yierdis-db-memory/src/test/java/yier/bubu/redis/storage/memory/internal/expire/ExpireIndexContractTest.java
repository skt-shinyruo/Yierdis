package yier.bubu.redis.storage.memory.internal.expire;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisKeyspace;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class ExpireIndexContractTest {
    @Test
    public void heapExpireIndexRoundTripsByteLookupAndClear() {
        YierdisHeapExpireIndex expires = new YierdisHeapExpireIndex();
        CanonicalKeyspace<Integer> keyspace = new CanonicalKeyspace<>();
        byte[] key = bytes("heap-key");
        keyspace.compute(key, (k, old) -> 1);

        assertByteRoundTripAndClear(expires, keyspace, key);
    }

    @Test
    public void ffmExpireIndexRoundTripsByteLookupAndClear() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-byte-contract")) {
            YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(runtime, "expire");
            CanonicalKeyspace<Integer> keyspace = new CanonicalKeyspace<>();
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(blobStore);
            try {
                byte[] key = bytes("ffm-key");
                keyspace.compute(key, (keyBytes, old) -> 1);

                assertByteRoundTripAndClear(expires, keyspace, key);
                Assert.assertEquals(0L, expires.nativeBytes());
            } finally {
                expires.clear();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void ffmExpireIndexRoundTripsNativeHandleLookupAndClear() {
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
                Assert.assertNull(expires.get(key));
                Assert.assertNull(expires.randomKey());
                Assert.assertNull(expires.randomKeyHandle());
            } finally {
                expires.clear();
                allocator.free(entry.nativeHandle());
            }
        }
    }

    private static void assertByteRoundTripAndClear(
            YierdisExpireIndex expires,
            YierdisKeyspace<Integer> keyspace,
            byte[] key
    ) {
        Assert.assertEquals(0, expires.size());

        long expireAt = 123456789L;
        expires.setExpireAtMillis(key, expireAt, keyspace);

        Assert.assertEquals(1, expires.size());
        Assert.assertEquals(Long.valueOf(expireAt), expires.get(key));
        Assert.assertNotNull(expires.randomKey());

        expires.removeExpire(key);
        Assert.assertNull(expires.get(key));
        Assert.assertEquals(0, expires.size());

        expires.setExpireAtMillis(key, expireAt + 1, keyspace);
        Assert.assertEquals(Long.valueOf(expireAt + 1), expires.get(key));
        expires.clear();
        Assert.assertEquals(0, expires.size());
        Assert.assertNull(expires.get(key));
        Assert.assertNull(expires.randomKey());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class CanonicalKeyspace<V> implements YierdisKeyspace<V> {
        private final Map<ByteArrayKey, Entry<V>> entries = new HashMap<>();

        @Override
        public int size() {
            return entries.size();
        }

        @Override
        public V get(byte[] key) {
            Entry<V> entry = entries.get(ByteArrayKey.of(key));
            return entry == null ? null : entry.value;
        }

        @Override
        public V get(BytesView key) {
            Entry<V> entry = entries.get(ByteArrayKey.of(key));
            return entry == null ? null : entry.value;
        }

        @Override
        public V get(KeyHandle keyHandle) {
            return get((BytesView) keyHandle);
        }

        @Override
        public KeyHandle keyHandle(byte[] key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyHandle keyHandle(BytesView key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] canonicalKey(byte[] key) {
            Entry<V> entry = entries.get(ByteArrayKey.of(key));
            return entry == null ? null : entry.key;
        }

        @Override
        public byte[] canonicalKey(BytesView key) {
            Entry<V> entry = entries.get(ByteArrayKey.of(key));
            return entry == null ? null : entry.key;
        }

        @Override
        public V compute(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
            Entry<V> oldEntry = entries.get(ByteArrayKey.of(key));
            V newValue = remappingFunction.apply(key, oldEntry == null ? null : oldEntry.value);
            if (newValue == null) {
                entries.remove(ByteArrayKey.of(key));
                return null;
            }
            entries.put(ByteArrayKey.of(key), new Entry<>(key, newValue));
            return newValue;
        }

        @Override
        public V computeIfPresent(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
            Entry<V> oldEntry = entries.get(ByteArrayKey.of(key));
            if (oldEntry == null) {
                return null;
            }
            return compute(oldEntry.key, remappingFunction);
        }

        @Override
        public boolean remove(byte[] key, V expectedValue) {
            Entry<V> entry = entries.get(ByteArrayKey.of(key));
            if (entry == null || entry.value != expectedValue) {
                return false;
            }
            entries.remove(ByteArrayKey.of(key));
            return true;
        }

        @Override
        public boolean remove(KeyHandle keyHandle, V expectedValue) {
            return remove(copy(keyHandle), expectedValue);
        }

        @Override
        public void clear() {
            entries.clear();
        }

        @Override
        public void forEach(BiConsumer<byte[], V> consumer) {
            entries.values().forEach(entry -> consumer.accept(entry.key, entry.value));
        }

        @Override
        public void forEachKeyHandle(BiConsumer<KeyHandle, V> consumer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer<V> consumer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] randomKey() {
            return entries.isEmpty() ? null : entries.values().iterator().next().key;
        }

        @Override
        public KeyHandle randomKeyHandle() {
            throw new UnsupportedOperationException();
        }

        private byte[] copy(KeyHandle keyHandle) {
            byte[] out = new byte[keyHandle.len()];
            for (int i = 0; i < out.length; i++) {
                out[i] = keyHandle.byteAt(i);
            }
            return out;
        }
    }

    private record Entry<V>(byte[] key, V value) {
    }

    private static final class ByteArrayKey {
        private final byte[] bytes;
        private final int hash;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        private static ByteArrayKey of(byte[] bytes) {
            return new ByteArrayKey(bytes);
        }

        private static ByteArrayKey of(BytesView view) {
            byte[] bytes = new byte[view.length()];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = view.getByte(i);
            }
            return new ByteArrayKey(bytes);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ByteArrayKey other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
