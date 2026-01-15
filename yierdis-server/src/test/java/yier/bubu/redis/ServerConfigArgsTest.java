package yier.bubu.redis;

import org.junit.Assert;
import org.junit.Test;

public class ServerConfigArgsTest {
    @Test
    public void helpReturnsNull() {
        Assert.assertNull(ServerConfig.fromArgs(new String[]{"--help"}));
    }

    @Test
    public void invalidWatermarkOrderFailsFast() {
        assertThrows(IllegalArgumentException.class, () -> ServerConfig.fromArgs(new String[]{
                "--backpressureHigh", "10",
                "--backpressureLow", "10"
        }));
    }

    @Test
    public void invalidMaxmemoryPolicyFailsFast() {
        assertThrows(IllegalArgumentException.class, () -> ServerConfig.fromArgs(new String[]{
                "--maxmemoryPolicy", "random-evict"
        }));
    }

    private static void assertThrows(Class<? extends Throwable> expected, Runnable r) {
        try {
            r.run();
            Assert.fail("expected exception: " + expected.getSimpleName());
        } catch (Throwable t) {
            if (!expected.isInstance(t)) {
                Assert.fail("expected " + expected.getSimpleName() + ", got: " + t.getClass().getName());
            }
        }
    }
}

