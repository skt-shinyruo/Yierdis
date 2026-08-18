package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;

public class YierdisFfmMemoryRuntimeTest {
    @Test
    public void regionLifecycleUpdatesRuntimeAccounting() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test-runtime")) {
            YierdisFfmRegion region = runtime.allocateRegion("test-region", 32);
            Assert.assertEquals(32L, runtime.usedBytes());
            region.close();
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void regionReadsAndWritesUseValueLayouts() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test-runtime")) {
            YierdisFfmRegion region = runtime.allocateRegion("test-region", 8);
            region.setLong(0, 42L);
            Assert.assertEquals(42L, region.getLong(0));
            region.close();
        }
    }
}
