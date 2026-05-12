package yier.bubu.redis.storage.memory.internal.ffm;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;

import java.nio.charset.StandardCharsets;

public class YierdisFfmBlobStoreTest {
    @Test
    public void storeRetainReleaseTracksLiveBytesUntilFinalRelease() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("blob-store-test")) {
            YierdisFfmBlobStore store = new YierdisFfmBlobStore(runtime, "blob");
            byte[] bytes = bytes("native-value");

            YierdisFfmBytesRef ref = store.store(bytes);
            long liveBytes = store.liveBytes();
            Assert.assertTrue(liveBytes >= bytes.length);
            Assert.assertArrayEquals(bytes, store.toByteArray(ref));
            Assert.assertEquals(liveBytes, runtime.usedBytes());

            store.retain(ref);
            Assert.assertEquals(liveBytes, store.liveBytes());

            store.release(ref);
            Assert.assertEquals(liveBytes, store.liveBytes());

            store.release(ref);
            Assert.assertEquals(0L, store.liveBytes());
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void emptyBlobUsesOneNativeByteButCopiesAsEmptyArray() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("blob-empty-test")) {
            YierdisFfmBlobStore store = new YierdisFfmBlobStore(runtime, "blob");
            YierdisFfmBytesRef ref = store.store(new byte[0]);

            Assert.assertEquals(0, ref.length());
            Assert.assertEquals(1L, store.liveBytes());
            Assert.assertArrayEquals(new byte[0], store.toByteArray(ref));

            store.release(ref);
            Assert.assertEquals(0L, store.liveBytes());
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void releasingUnknownRefFailsWithoutChangingLiveBytes() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("blob-unknown-test")) {
            YierdisFfmBlobStore store = new YierdisFfmBlobStore(runtime, "blob");
            YierdisFfmBytesRef external = YierdisFfmBlobStore.fromBytes(runtime, bytes("external"));
            try {
                Assert.assertEquals(0L, store.liveBytes());
                try {
                    store.release(external);
                    Assert.fail("expected unknown ref release to fail");
                } catch (IllegalStateException expected) {
                    Assert.assertEquals("unknown blob ref", expected.getMessage());
                }
                Assert.assertEquals(0L, store.liveBytes());
            } finally {
                external.region().close();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
