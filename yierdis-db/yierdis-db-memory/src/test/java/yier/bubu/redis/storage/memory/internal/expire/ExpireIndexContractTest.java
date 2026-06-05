package yier.bubu.redis.storage.memory.internal.expire;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmKeyspace;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.ByteArrayKeyspace;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisKeyspace;

import java.nio.charset.StandardCharsets;

public class ExpireIndexContractTest {
    @Test
    public void heapExpireIndexRoundTripsByteLookupAndClear() {
        YierdisHeapExpireIndex expires = new YierdisHeapExpireIndex();
        ByteArrayKeyspace<Integer> keyspace = new ByteArrayKeyspace<>();
        byte[] key = bytes("heap-key");
        keyspace.compute(key, (k, old) -> 1);

        assertByteRoundTripAndClear(expires, keyspace, key);
    }

    @Test
    public void ffmExpireIndexRoundTripsByteLookupAndClear() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-byte-contract")) {
            YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(runtime, "expire");
            YierdisFfmKeyspace<Integer> keyspace = new YierdisFfmKeyspace<>(blobStore);
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(blobStore);
            try {
                byte[] key = bytes("ffm-key");
                keyspace.compute(key, (keyBytes, old) -> 1);

                assertByteRoundTripAndClear(expires, keyspace, key);
                Assert.assertEquals(0L, expires.nativeBytes());
            } finally {
                expires.clear();
                keyspace.clear();
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
}
