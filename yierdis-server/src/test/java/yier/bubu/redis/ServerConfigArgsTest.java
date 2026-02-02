package yier.bubu.redis;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.args.YierdisCliException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

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

    @Test
    public void validateErrorsPrintUsageToStderr() {
        String err = captureStderr(() -> assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                "--offheapBackend", "netty",
                "--offheapKeysEnabled"
        })));

        Assert.assertTrue("stderr should include usage", err.contains("Usage: yierdis"));
        Assert.assertTrue("stderr should include flag name", err.contains("--offheapKeysEnabled") || err.contains("offheapKeysEnabled"));
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

    private static String captureStderr(Runnable r) {
        PrintStream prev = System.err;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
        System.setErr(ps);
        try {
            r.run();
        } finally {
            System.setErr(prev);
            ps.flush();
        }
        return baos.toString(StandardCharsets.UTF_8);
    }
}
