package yier.bubu.redis.storage.memory.internal.ffm;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import java.nio.charset.StandardCharsets;

public class YierdisFfmKeyspaceTest {
    @Test
    public void computeKeepsExistingEntryWhenRemappingThrows() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("ffm-keyspace-existing")) {
            YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(runtime, "keys");
            YierdisFfmKeyspace<Integer> keyspace = new YierdisFfmKeyspace<>(blobStore);
            try {
                byte[] key = bytes("existing-key");
                keyspace.compute(key, (keyBytes, old) -> 1);
                long liveBytes = blobStore.liveBytes();

                RuntimeException failure = new RuntimeException("boom");
                try {
                    keyspace.compute(key, (keyBytes, old) -> {
                        Assert.assertArrayEquals(key, keyBytes);
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
