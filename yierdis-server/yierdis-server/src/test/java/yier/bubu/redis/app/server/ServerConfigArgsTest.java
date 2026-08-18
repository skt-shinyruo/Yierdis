package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.args.YierdisCliException;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ServerConfigArgsTest {
    @Test
    public void helpPrintsUsageAndReturnsNull() {
        String out = captureStdout(() -> Assert.assertNull(ServerConfig.fromArgs(new String[]{"--help"})));
        Assert.assertTrue("stdout should include usage", out.contains("Usage: yierdis"));
    }

    @Test
    public void maxmemoryMustBeSpecifiedExplicitly() {
        YierdisCliException error = assertThrows(
                YierdisCliException.class,
                () -> ServerConfig.fromArgs(new String[]{"--port", "0"})
        );

        Assert.assertTrue(error.getMessage().contains("--maxmemoryBytes must be specified explicitly"));
    }

    @Test
    public void invalidWatermarkOrderFailsFast() {
        assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                "--maxmemoryBytes", "0",
                "--backpressureHigh", "10",
                "--backpressureLow", "10"
        }));
    }

    @Test
    public void invalidMaxmemoryPolicyFailsFast() {
        assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                "--maxmemoryBytes", "0",
                "--maxmemoryPolicy", "random-evict"
        }));
    }

    @Test
    public void parseErrorsPrintUsageAndThrowCliException() {
        String err = captureStderr(() -> {
            assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                    "--port", "not-a-number"
            }));
        });

        Assert.assertTrue("stderr should include usage", err.contains("Usage: yierdis"));
        Assert.assertTrue("stderr should include flag name", err.contains("--port"));
    }

    @Test
    public void normalizedArgsExposeSharedRuntimeConfig() {
        YierdisServerRuntimeConfig config = ServerConfig.fromArgs(new String[]{
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
                "--protocolMaxCommandBytes", "65536",
                "--maxmemoryBytes", "1048576",
                "--maxmemoryScope", "perdb",
                "--maxmemoryPolicy", "ALLKEYS-RANDOM",
                "--maxmemorySamples", "9",
                "--evictionTimeLimitMillis", "11",
                "--expireCleanupTimeLimitMillis", "13",
                "--nativeDefragEnabled",
                "--nativeDefragMaxMoveBytes", "1024",
                "--nativeDefragMaxObjects", "7",
                "--nativeDefragTimeLimitMillis", "3",
                "--keysTimeBudgetMillis", "17",
                "--keysMaxResults", "23"
        });

        Map<String, Object> runtimeConfig = recordValues(config);
        Assert.assertEquals(6380, runtimeConfig.get("port"));
        Assert.assertEquals(32, runtimeConfig.get("databases"));
        Assert.assertEquals(0L, runtimeConfig.get("cleanupIntervalMillis"));
        Assert.assertEquals(4, runtimeConfig.get("ioThreads"));
        Assert.assertEquals(2048, runtimeConfig.get("executorQueueCapacity"));
        Assert.assertEquals(4096L, runtimeConfig.get("executorQueueMaxBytes"));
        Assert.assertEquals(SchedulingPolicy.GLOBAL, runtimeConfig.get("executorSchedulingPolicy"));
        Assert.assertEquals(512, runtimeConfig.get("backpressureHighWatermark"));
        Assert.assertEquals(64, runtimeConfig.get("backpressureLowWatermark"));
        Assert.assertEquals(8192L, runtimeConfig.get("backpressureBytesHighWatermark"));
        Assert.assertEquals(2048L, runtimeConfig.get("backpressureBytesLowWatermark"));
        Assert.assertEquals(256, runtimeConfig.get("executorMaxDrainCommands"));
        Assert.assertEquals(7L, runtimeConfig.get("executorDrainTimeLimitMillis"));
        Assert.assertEquals(128, runtimeConfig.get("transactionQueueMaxCommands"));
        Assert.assertEquals(16384L, runtimeConfig.get("transactionQueueMaxBytes"));
        Assert.assertEquals(32768, runtimeConfig.get("protocolMaxBulkBytes"));
        Assert.assertEquals(128, runtimeConfig.get("protocolMaxArgs"));
        Assert.assertEquals(4096, runtimeConfig.get("protocolMaxLineBytes"));
        Assert.assertEquals(65536, runtimeConfig.get("protocolMaxCommandBytes"));
        Assert.assertEquals(1048576L, runtimeConfig.get("maxmemoryBytes"));
        Assert.assertEquals(YierdisInstanceConfig.MaxmemoryScope.PER_DB, runtimeConfig.get("maxmemoryScope"));
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, runtimeConfig.get("maxmemoryPolicy"));
        Assert.assertEquals(9, runtimeConfig.get("maxmemorySamples"));
        Assert.assertEquals(11L, runtimeConfig.get("evictionTimeLimitMillis"));
        Assert.assertEquals(13L, runtimeConfig.get("expireCleanupTimeLimitMillis"));
        Assert.assertEquals(true, runtimeConfig.get("nativeDefragEnabled"));
        Assert.assertEquals(1024L, runtimeConfig.get("nativeDefragMaxMoveBytes"));
        Assert.assertEquals(7L, runtimeConfig.get("nativeDefragMaxObjects"));
        Assert.assertEquals(3L, runtimeConfig.get("nativeDefragTimeLimitMillis"));
        Assert.assertEquals(17L, runtimeConfig.get("keysTimeBudgetMillis"));
        Assert.assertEquals(23, runtimeConfig.get("keysMaxResults"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapBackend"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapMaxBytes"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapKeysEnabled"));
    }

    @Test
    public void maxmemoryPolicyUnderscoreInputNormalizesToCoreEnum() {
        YierdisServerRuntimeConfig config = ServerConfig.fromArgs(new String[]{
                "--maxmemoryBytes", "0",
                "--maxmemoryPolicy", "ALLKEYS_RANDOM"
        });

        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, config.maxmemoryPolicy());
    }

    @Test
    public void validateErrorsPrintUsageToStderr() {
        String err = captureStderr(() -> {
            assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                    "--offheapBackend", "foreign"
            }));
        });

        Assert.assertTrue("stderr should include usage", err.contains("Usage: yierdis"));
        Assert.assertTrue("stderr should include flag name", err.contains("--offheapBackend") || err.contains("offheapBackend"));
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

    private static Map<String, Object> recordValues(Object record) {
        Assert.assertTrue(record.getClass().isRecord());
        Map<String, Object> values = new HashMap<>();
        for (RecordComponent component : record.getClass().getRecordComponents()) {
            try {
                values.put(component.getName(), component.getAccessor().invoke(record));
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("failed to read record component: " + component.getName(), e);
            }
        }
        return values;
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
