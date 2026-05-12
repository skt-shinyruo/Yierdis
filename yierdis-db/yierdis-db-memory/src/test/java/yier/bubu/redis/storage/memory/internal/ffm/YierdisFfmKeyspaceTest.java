package yier.bubu.redis.storage.memory.internal.ffm;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

import java.nio.charset.StandardCharsets;

public class YierdisFfmKeyspaceTest {
    @Test
    public void computeWithHandleReleasesNewKeyWhenRemappingThrows() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-keyspace-rollback")) {
            YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(runtime, "keys");
            YierdisFfmKeyspace<Integer> keyspace = new YierdisFfmKeyspace<>(blobStore);

            RuntimeException failure = new RuntimeException("boom");
            try {
                keyspace.computeWithHandle(bytes("new-key"), (handle, old) -> {
                    Assert.assertNotNull(handle);
                    Assert.assertNull(old);
                    throw failure;
                });
                Assert.fail("expected remapping failure");
            } catch (RuntimeException actual) {
                Assert.assertSame(failure, actual);
            }

            Assert.assertEquals(0, keyspace.size());
            Assert.assertNull(keyspace.get(bytes("new-key")));
            Assert.assertEquals(0L, blobStore.liveBytes());
            keyspace.clear();
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void computeWithHandleKeepsExistingEntryWhenRemappingThrows() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-keyspace-existing")) {
            YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(runtime, "keys");
            YierdisFfmKeyspace<Integer> keyspace = new YierdisFfmKeyspace<>(blobStore);
            try {
                byte[] key = bytes("existing-key");
                keyspace.computeWithHandle(key, (handle, old) -> 1);
                long liveBytes = blobStore.liveBytes();
                KeyHandle before = keyspace.keyHandle(key);
                Assert.assertNotNull(before);

                RuntimeException failure = new RuntimeException("boom");
                try {
                    keyspace.computeWithHandle(key, (handle, old) -> {
                        Assert.assertEquals(before, handle);
                        Assert.assertEquals(Integer.valueOf(1), old);
                        throw failure;
                    });
                    Assert.fail("expected remapping failure");
                } catch (RuntimeException actual) {
                    Assert.assertSame(failure, actual);
                }

                Assert.assertEquals(Integer.valueOf(1), keyspace.get(key));
                Assert.assertEquals(1, keyspace.size());
                Assert.assertEquals(liveBytes, blobStore.liveBytes());
            } finally {
                keyspace.clear();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
