package yier.bubu.redis.storage.memory.internal.ffm;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;

import java.nio.charset.StandardCharsets;

public class YierdisFfmRehashConsistencyTest {
    @Test
    public void keyspaceGetRemainsReadableWhileShrinkRehashFinishes() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test")) {
            YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(runtime, "ffm-keyspace");
            YierdisFfmKeyspace<Integer> keyspace = new YierdisFfmKeyspace<>(blobStore);
            try {
                int n = 512;
                for (int i = 0; i < n; i++) {
                    byte[] key = bytes("k" + i);
                    int value = i;
                    keyspace.compute(key, (k, old) -> value);
                }
                Assert.assertEquals(n, keyspace.size());

                for (int i = 0; i < 412; i++) {
                    byte[] key = bytes("k" + i);
                    keyspace.computeIfPresent(key, (k, old) -> null);
                }

                byte[] keepKey = bytes("k500");
                Assert.assertTrue("expected shrink rehash to start", keyspace.isRehashing());
                while (keyspace.isRehashing()) {
                    Assert.assertEquals(Integer.valueOf(500), keyspace.get(keepKey));
                }
                Assert.assertEquals(Integer.valueOf(500), keyspace.get(keepKey));
            } finally {
                keyspace.clear();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void expireIndexRandomKeysRemainResolvableWhileShrinkRehashFinishes() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test")) {
            YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(runtime, "ffm-expire");
            YierdisFfmKeyspace<Integer> store = new YierdisFfmKeyspace<>(blobStore);
            YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(blobStore);
            try {
                int n = 200;
                long nowMillis = System.currentTimeMillis();
                for (int i = 0; i < n; i++) {
                    byte[] key = bytes("k" + i);
                    int value = i;
                    store.compute(key, (k, old) -> value);
                    expires.setExpireAtMillis(key, nowMillis, store);
                }
                Assert.assertEquals(n, store.size());
                Assert.assertEquals(n, expires.size());

                int removed = 0;
                while (expires.size() > 0) {
                    byte[] key = expires.randomKey();
                    Assert.assertNotNull(key);
                    Assert.assertNotNull("randomKey() returned a key that get() could not resolve", expires.get(key));
                    Integer value = store.get(key);
                    Assert.assertNotNull(value);
                    expires.removeExpire(key);
                    Assert.assertTrue("store entry must be removable after expire removal", store.remove(key, value));
                    removed++;
                }

                Assert.assertEquals(n, removed);
                Assert.assertEquals(0, store.size());
                Assert.assertEquals(0, expires.size());
            } finally {
                expires.clear();
                store.clear();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
