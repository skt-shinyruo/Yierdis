package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.args.YierdisCliException;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ServerConfigArgsTest {
    @Test
    public void helpFlagIsRejectedWithoutPrintingUsage() {
        String err = captureStderr(() -> assertThrows(
                YierdisCliException.class,
                () -> ServerConfig.fromArgs(new String[]{"--help"})
        ));

        Assert.assertTrue("stderr should name the rejected flag", err.contains("--help"));
        Assert.assertFalse("server jar prints no usage", err.contains("Usage"));
    }

    @Test
    public void missingConfigFileIsRejected() {
        YierdisCliException error = assertThrows(
                YierdisCliException.class,
                () -> ServerConfig.fromArgs(new String[]{"--config", "/nonexistent/yierdis.conf"})
        );

        Assert.assertTrue(error.getMessage().contains("configuration file not found"));
    }

    @Test
    public void maxmemoryMustBeSpecifiedExplicitly() throws IOException {
        YierdisCliException error = assertThrows(
                YierdisCliException.class,
                () -> fromConfig("port", "0")
        );

        Assert.assertTrue(error.getMessage().contains("maxmemoryBytes must be specified explicitly"));
    }

    @Test
    public void invalidWatermarkOrderFailsFast() throws IOException {
        assertThrows(YierdisCliException.class, () -> fromConfig(
                "maxmemoryBytes", "0",
                "backpressureHigh", "10",
                "backpressureLow", "10"
        ));
    }

    @Test
    public void invalidMaxmemoryPolicyFailsFast() throws IOException {
        assertThrows(YierdisCliException.class, () -> fromConfig(
                "maxmemoryBytes", "0",
                "maxmemoryPolicy", "random-evict"
        ));
    }

    @Test
    public void parseErrorsReportTheReasonWithoutUsage() throws IOException {
        String path = writeConfig("port", "not-a-number");
        String err = captureStderr(() -> assertThrows(
                YierdisCliException.class,
                () -> ServerConfig.fromArgs(new String[]{"--config", path})
        ));

        Assert.assertTrue("stderr should include key name", err.contains("port"));
        Assert.assertTrue("stderr should include the offending value", err.contains("not-a-number"));
        Assert.assertFalse("server jar prints no usage", err.contains("Usage"));
    }

    @Test
    public void normalizedArgsExposeSharedRuntimeConfig() throws IOException {
        YierdisServerRuntimeConfig config = fromConfig(
                "port", "6380",
                "databases", "32",
                "noCleanup",
                "ioThreads", "4",
                "executorQueueCapacity", "2048",
                "executorQueueMaxBytes", "4096",
                "executorSchedulingPolicy", "GLOBAL",
                "backpressureHigh", "512",
                "backpressureLow", "64",
                "backpressureBytesHigh", "8192",
                "backpressureBytesLow", "2048",
                "executorMaxDrain", "256",
                "executorDrainMillis", "7",
                "transactionQueueMaxCommands", "128",
                "transactionQueueMaxBytes", "16384",
                "protocolMaxBulkBytes", "32768",
                "protocolMaxArgs", "128",
                "protocolMaxLineBytes", "4096",
                "protocolMaxCommandBytes", "65536",
                "maxmemoryBytes", "1048576",
                "maxmemoryScope", "perdb",
                "maxmemoryPolicy", "ALLKEYS-RANDOM",
                "maxmemorySamples", "9",
                "evictionTimeLimitMillis", "11",
                "expireCleanupTimeLimitMillis", "13",
                "nativeDefragEnabled",
                "nativeDefragMaxMoveBytes", "1024",
                "nativeDefragMaxObjects", "7",
                "nativeDefragTimeLimitMillis", "3",
                "keysTimeBudgetMillis", "17",
                "keysMaxResults", "23"
        );

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
    public void maxmemoryPolicyUnderscoreInputNormalizesToCoreEnum() throws IOException {
        YierdisServerRuntimeConfig config = fromConfig(
                "maxmemoryBytes", "0",
                "maxmemoryPolicy", "ALLKEYS_RANDOM"
        );

        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, config.maxmemoryPolicy());
    }

    @Test
    public void unknownKeysReportTheReasonWithoutUsage() throws IOException {
        String path = writeConfig("offheapBackend", "foreign");
        String err = captureStderr(() -> assertThrows(
                YierdisCliException.class,
                () -> ServerConfig.fromArgs(new String[]{"--config", path})
        ));

        Assert.assertTrue("stderr should include key name", err.contains("offheapBackend"));
        Assert.assertFalse("server jar prints no usage", err.contains("Usage"));
    }

    private static YierdisServerRuntimeConfig fromConfig(String... kv) throws IOException {
        return ServerConfig.fromArgs(new String[]{"--config", writeConfig(kv)});
    }

    private static String writeConfig(String... kv) throws IOException {
        Properties props = new Properties();
        for (int i = 0; i < kv.length; i++) {
            String key = kv[i];
            if (key.equals("noCleanup") || key.equals("nativeDefragEnabled")) {
                props.setProperty(key, "true");
            } else {
                props.setProperty(key, kv[++i]);
            }
        }
        Path path = Files.createTempFile("yierdis-test", ".conf");
        path.toFile().deleteOnExit();
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            props.store(writer, null);
        }
        return path.toString();
    }

    private static <T extends Throwable> T assertThrows(Class<T> expected, ThrowableRunnable r) {
        try {
            r.run();
            Assert.fail("expected exception: " + expected.getSimpleName());
            return null;
        } catch (Throwable t) {
            if (!expected.isInstance(t)) {
                throw new AssertionError("expected " + expected.getSimpleName() + ", got: " + t.getClass().getName(), t);
            }
            return expected.cast(t);
        }
    }

    private interface ThrowableRunnable {
        void run() throws Exception;
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

    private static String captureStderr(Runnable r) {
        PrintStream previous = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            r.run();
        } finally {
            System.setErr(previous);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
