package yier.bubu.redis.storage.memory.internal.expire;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmKeyspace;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.ByteArrayKeyspace;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisKeyspace;

import java.nio.charset.StandardCharsets;

public class ExpireIndexContractTest {
    @Test
    public void heapExpireIndexRoundTripsHandleLookupAndClear() {
        YierdisHeapExpireIndex expires = new YierdisHeapExpireIndex();
        ByteArrayKeyspace<Integer> keyspace = new ByteArrayKeyspace<>();
        byte[] key = bytes("heap-key");
        keyspace.compute(key, (k, old) -> 1);
        KeyHandle handle = keyspace.keyHandle(key);

        assertRoundTripAndClear(expires, keyspace, key, handle);
    }

    @Test
    public void ffmExpireIndexRoundTripsHandleLookupAndClear() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-expire-contract")) {
            YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(runtime, "expire");
            YierdisFfmKeyspace<Integer> keyspace = new YierdisFfmKeyspace<>(blobStore);
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(blobStore);
            try {
                byte[] key = bytes("ffm-key");
                keyspace.computeWithHandle(key, (handle, old) -> 1);
                KeyHandle handle = keyspace.keyHandle(key);

                assertRoundTripAndClear(expires, keyspace, key, handle);
                Assert.assertEquals(0L, expires.nativeBytes());
            } finally {
                expires.clear();
                keyspace.clear();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static void assertRoundTripAndClear(
            YierdisExpireIndex expires,
            YierdisKeyspace<Integer> keyspace,
            byte[] key,
            KeyHandle handle
    ) {
        Assert.assertNotNull(handle);
        Assert.assertEquals(0, expires.size());

        long expireAt = 123456789L;
        expires.setExpireAtMillis(key, expireAt, keyspace);

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
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
