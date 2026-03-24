package yier.bubu.redis.args;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.protocol.ProtocolLimits;

public class YierdisServerArgsTest {
    @Test
    public void helpParses() {
        YierdisServerArgs args = new YierdisServerArgs();
        new CommandLine(args).parseArgs("--help");
        Assert.assertTrue(args.help);
    }

    @Test
    public void normalizeLowercasesBackendAndPolicy() {
        YierdisServerArgs args = parse("--offheapBackend", "UNSAFE", "--maxmemoryPolicy", "ALLKEYS-LRU");
        args.normalizeAndValidate();
        Assert.assertEquals("unsafe", args.offheapBackend);
        Assert.assertEquals("allkeys-lru", args.maxmemoryPolicy);
    }

    @Test
    public void normalizedArgsConvertToStableRuntimeConfig() {
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
                "--offheapBackend", "UNSAFE",
                "--offheapMaxBytes", "65536",
                "--offheapKeysEnabled",
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
        Assert.assertEquals("unsafe", args.offheapBackend);
        Assert.assertEquals("per-db", args.maxmemoryScope);
        Assert.assertEquals("allkeys-random", args.maxmemoryPolicy);
        Assert.assertEquals(0, args.cleanupIntervalMillis);

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
                args.toRuntimeConfig()
        );
    }

    @Test
    public void toArgvRoundTripsNormalizedSettingsUsedByBench() {
        YierdisServerArgs args = parse(
                "--port", "6381",
                "--noCleanup",
                "--executorSchedulingPolicy", "FAIR",
                "--offheapBackend", "NETTY",
                "--offheapMaxBytes", "2048",
                "--maxmemoryScope", "perdb",
                "--maxmemoryPolicy", "ALLKEYS-LRU",
                "--keysTimeBudgetMillis", "0",
                "--keysMaxResults", "0"
        );

        args.normalizeAndValidate();

        YierdisServerArgs copied = args.copy();
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
    public void invalidOffheapBackendIsRejected() {
        YierdisServerArgs args = parse("--offheapBackend", "???");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void offheapBackendNoneWithNonzeroMaxBytesIsRejected() {
        YierdisServerArgs args = parse("--offheapBackend", "none", "--offheapMaxBytes", "1");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidMaxmemoryPolicyIsRejected() {
        YierdisServerArgs args = parse("--maxmemoryPolicy", "random-evict");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
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
