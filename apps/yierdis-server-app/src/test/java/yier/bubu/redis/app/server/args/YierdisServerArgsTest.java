package yier.bubu.redis.app.server.args;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.ops.MaxmemoryPolicy;
import yier.bubu.redis.protocol.ProtocolLimits;

import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;

public class YierdisServerArgsTest {
    @Test
    public void helpParses() {
        YierdisServerArgs args = new YierdisServerArgs();
        new CommandLine(args).parseArgs("--help");
        Assert.assertTrue(args.help);
    }

    @Test
    public void normalizeLowercasesSchedulingAndPolicy() {
        YierdisServerArgs args = parse("--executorSchedulingPolicy", "GLOBAL", "--maxmemoryPolicy", "ALLKEYS-LRU");
        args.normalizeAndValidate();
        Assert.assertEquals("global", args.executorSchedulingPolicy);
        Assert.assertEquals("allkeys-lru", args.maxmemoryPolicy);
    }

    @Test
    public void normalizedArgsConvertToRuntimeConfigWithoutLegacyOffheapFields() {
        YierdisServerArgs args = parse(
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
                "--maxmemoryBytes", "1048576",
                "--maxmemoryScope", "Per_Db",
                "--maxmemoryPolicy", "ALLKEYS-RANDOM",
                "--maxmemorySamples", "9",
                "--evictionTimeLimitMillis", "11",
                "--expireCleanupTimeLimitMillis", "13",
                "--keysTimeBudgetMillis", "17",
                "--keysMaxResults", "23"
        );

        args.normalizeAndValidate();

        Assert.assertEquals("global", args.executorSchedulingPolicy);
        Assert.assertEquals("per-db", args.maxmemoryScope);
        Assert.assertEquals("allkeys-random", args.maxmemoryPolicy);
        Assert.assertEquals(0, args.cleanupIntervalMillis);

        Map<String, Object> runtimeConfig = recordValues(args.toRuntimeConfig());
        Assert.assertEquals(6380, runtimeConfig.get("port"));
        Assert.assertEquals(32, runtimeConfig.get("databases"));
        Assert.assertEquals(0L, runtimeConfig.get("cleanupIntervalMillis"));
        Assert.assertEquals(4, runtimeConfig.get("ioThreads"));
        Assert.assertEquals(2048, runtimeConfig.get("executorQueueCapacity"));
        Assert.assertEquals(4096L, runtimeConfig.get("executorQueueMaxBytes"));
        Assert.assertEquals(YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.GLOBAL, runtimeConfig.get("executorSchedulingPolicy"));
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
        Assert.assertEquals(1048576L, runtimeConfig.get("maxmemoryBytes"));
        Assert.assertEquals(YierdisServerRuntimeConfig.MaxmemoryScope.PER_DB, runtimeConfig.get("maxmemoryScope"));
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, runtimeConfig.get("maxmemoryPolicy"));
        Assert.assertEquals(9, runtimeConfig.get("maxmemorySamples"));
        Assert.assertEquals(11L, runtimeConfig.get("evictionTimeLimitMillis"));
        Assert.assertEquals(13L, runtimeConfig.get("expireCleanupTimeLimitMillis"));
        Assert.assertEquals(17L, runtimeConfig.get("keysTimeBudgetMillis"));
        Assert.assertEquals(23, runtimeConfig.get("keysMaxResults"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapBackend"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapMaxBytes"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapKeysEnabled"));
    }

    @Test
    public void toArgvRoundTripsNormalizedSettingsUsedByBench() {
        YierdisServerArgs args = parse(
                "--port", "6381",
                "--noCleanup",
                "--executorSchedulingPolicy", "FAIR",
                "--maxmemoryScope", "perdb",
                "--maxmemoryPolicy", "ALLKEYS-LRU",
                "--keysTimeBudgetMillis", "0",
                "--keysMaxResults", "0"
        );

        args.normalizeAndValidate();

        YierdisServerArgs copied = args.copy();
        Assert.assertFalse(copied.toArgv().contains("--offheapBackend"));
        Assert.assertFalse(copied.toArgv().contains("--offheapMaxBytes"));
        Assert.assertFalse(copied.toArgv().contains("--offheapKeysEnabled"));
        Assert.assertEquals(args.toArgv(), copied.toArgv());

        YierdisServerArgs reparsed = parse(copied.toArgv().toArray(new String[0]));
        reparsed.normalizeAndValidate();

        Assert.assertEquals(copied.toArgv(), reparsed.toArgv());
        Assert.assertEquals(copied.toRuntimeConfig(), reparsed.toRuntimeConfig());
    }

    @Test
    public void invalidPortIsRejected() {
        YierdisServerArgs args = parse("--port", "-1");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidWatermarkOrderIsRejected() {
        YierdisServerArgs args = parse("--backpressureHigh", "10", "--backpressureLow", "10");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidBytesWatermarkOrderIsRejected() {
        YierdisServerArgs args = parse("--backpressureBytesHigh", "10", "--backpressureBytesLow", "10");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void bytesLowWithoutBytesHighIsRejected() {
        YierdisServerArgs args = parse("--backpressureBytesHigh", "0", "--backpressureBytesLow", "1");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void deletedOffheapFlagsAreRejectedAtParseTime() {
        YierdisServerArgs args = new YierdisServerArgs();
        assertThrows(CommandLine.ParameterException.class, () -> new CommandLine(args).parseArgs("--offheapBackend", "foreign"));
        assertThrows(CommandLine.ParameterException.class, () -> new CommandLine(args).parseArgs("--offheapMaxBytes", "1"));
        assertThrows(CommandLine.ParameterException.class, () -> new CommandLine(args).parseArgs("--offheapKeysEnabled"));
    }

    @Test
    public void invalidMaxmemoryPolicyIsRejected() {
        YierdisServerArgs args = parse("--maxmemoryPolicy", "random-evict");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void normalizeAcceptsCorePolicyUnderscoreAliases() {
        YierdisServerArgs args = parse("--maxmemoryPolicy", "ALLKEYS_RANDOM");

        args.normalizeAndValidate();

        Assert.assertEquals("allkeys-random", args.maxmemoryPolicy);
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, args.toRuntimeConfig().maxmemoryPolicy());
    }

    @Test
    public void protocolDefaultsMatchProtocolLimitsSsot() {
        YierdisServerArgs args = new YierdisServerArgs();
        Assert.assertEquals(ProtocolLimits.DEFAULT_MAX_REQUEST_PAYLOAD_BYTES, args.protocolMaxBulkBytes);
        Assert.assertEquals(ProtocolLimits.DEFAULT_MAX_ARGS, args.protocolMaxArgs);
        Assert.assertEquals(ProtocolLimits.DEFAULT_MAX_HEADER_BYTES, args.protocolMaxLineBytes);
    }

    private static YierdisServerArgs parse(String... argv) {
        YierdisServerArgs args = new YierdisServerArgs();
        new CommandLine(args).parseArgs(argv);
        return args;
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
