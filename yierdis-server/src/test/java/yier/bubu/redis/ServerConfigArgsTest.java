package yier.bubu.redis;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.args.YierdisCliException;
import yier.bubu.redis.args.YierdisServerRuntimeConfig;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class ServerConfigArgsTest {
    @Test
    public void helpPrintsUsageAndReturnsNull() {
        String out = captureStdout(() -> Assert.assertNull(ServerConfig.fromArgs(new String[]{"--help"})));
        Assert.assertTrue("stdout should include usage", out.contains("Usage: yierdis"));
    }

    @Test
    public void invalidWatermarkOrderFailsFast() {
        YierdisCliException error = assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                "--backpressureHigh", "10",
                "--backpressureLow", "10"
        }));
        Assert.assertEquals(2, error.exitCode());
        Assert.assertTrue(error.shouldPrintUsage());
    }

    @Test
    public void invalidMaxmemoryPolicyFailsFast() {
        YierdisCliException error = assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                "--maxmemoryPolicy", "random-evict"
        }));
        Assert.assertEquals(2, error.exitCode());
        Assert.assertTrue(error.shouldPrintUsage());
    }

    @Test
    public void parseErrorsPrintUsageAndThrowCliException() {
        String err = captureStderr(() -> {
            YierdisCliException error = assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                    "--port", "not-a-number"
            }));
            Assert.assertEquals(2, error.exitCode());
            Assert.assertTrue(error.shouldPrintUsage());
        });

        Assert.assertTrue("stderr should include usage", err.contains("Usage: yierdis"));
        Assert.assertTrue("stderr should include flag name", err.contains("--port"));
    }

    @Test
    public void normalizedArgsExposeSharedRuntimeConfig() {
        ServerConfig config = ServerConfig.fromArgs(new String[]{
                "--port", "6380",
                "--databases", "32",
                "--noCleanup",
                "--ioThreads", "4",
                "--executorQueueCapacity", "2048",
                "--executorQueueMaxBytes", "4096",
                "--executorSchedulingPolicy", "GLOBAL",
                "--backpressureHigh", "512",
                "--backpressureLow", "64",
                "--backpressureBytesHigh", "8192",
                "--backpressureBytesLow", "2048",
                "--executorMaxDrain", "256",
                "--executorDrainMillis", "7",
                "--transactionQueueMaxCommands", "128",
                "--transactionQueueMaxBytes", "16384",
                "--protocolMaxBulkBytes", "32768",
                "--protocolMaxArgs", "128",
                "--protocolMaxLineBytes", "4096",
                "--offheapBackend", "UNSAFE",
                "--offheapMaxBytes", "65536",
                "--offheapKeysEnabled",
                "--maxmemoryBytes", "1048576",
                "--maxmemoryScope", "perdb",
                "--maxmemoryPolicy", "ALLKEYS-RANDOM",
                "--maxmemorySamples", "9",
                "--evictionTimeLimitMillis", "11",
                "--expireCleanupTimeLimitMillis", "13",
                "--keysTimeBudgetMillis", "17",
                "--keysMaxResults", "23"
        });

        Assert.assertEquals(
                new YierdisServerRuntimeConfig(
                        6380,
                        32,
                        0,
                        4,
                        2048,
                        4096,
                        YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.GLOBAL,
                        512,
                        64,
                        8192,
                        2048,
                        256,
                        7,
                        128,
                        16384,
                        32768,
                        128,
                        4096,
                        YierdisServerRuntimeConfig.OffheapBackend.UNSAFE,
                        65536,
                        true,
                        1048576,
                        YierdisServerRuntimeConfig.MaxmemoryScope.PER_DB,
                        YierdisServerRuntimeConfig.MaxmemoryPolicy.ALLKEYS_RANDOM,
                        9,
                        11,
                        13,
                        17,
                        23
                ),
                config.runtimeConfig()
        );
    }

    @Test
    public void validateErrorsPrintUsageToStderr() {
        String err = captureStderr(() -> {
            YierdisCliException error = assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                    "--offheapBackend", "netty",
                    "--offheapKeysEnabled"
            }));
            Assert.assertEquals(2, error.exitCode());
            Assert.assertTrue(error.shouldPrintUsage());
        });

        Assert.assertTrue("stderr should include usage", err.contains("Usage: yierdis"));
        Assert.assertTrue("stderr should include flag name", err.contains("--offheapKeysEnabled") || err.contains("offheapKeysEnabled"));
    }

    private static <T extends Throwable> T assertThrows(Class<T> expected, Runnable r) {
        try {
            r.run();
            Assert.fail("expected exception: " + expected.getSimpleName());
            return null;
        } catch (Throwable t) {
            if (!expected.isInstance(t)) {
                Assert.fail("expected " + expected.getSimpleName() + ", got: " + t.getClass().getName());
            }
            return expected.cast(t);
        }
    }

    private static String captureStdout(Runnable r) {
        PrintStream prev = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
        System.setOut(ps);
        try {
            r.run();
        } finally {
            System.setOut(prev);
            ps.flush();
        }
        return baos.toString(StandardCharsets.UTF_8);
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
