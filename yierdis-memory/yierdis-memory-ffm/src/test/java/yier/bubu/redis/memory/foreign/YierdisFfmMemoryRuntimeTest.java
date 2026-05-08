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
    public void spanReadsAndWritesUseValueLayouts() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test-runtime")) {
            YierdisFfmRegion region = runtime.allocateRegion("test-region", 8);
            YierdisFfmSpan span = region.span(0, 8);
            YierdisFfmAccess.setByte(span, 0, (byte) 'a');
            YierdisFfmAccess.setLong(span, 0, 42L);
            Assert.assertEquals(42L, YierdisFfmAccess.getLong(span, 0));
            region.close();
        }
    }
}
