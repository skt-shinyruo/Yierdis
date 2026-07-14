package yier.bubu.redis.app.server.args;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

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
                "--protocolMaxCommandBytes", "65536",
                "--protocolGlobalInFlightBytes", "1048576",
                "--maxmemoryBytes", "1048576",
                "--maxmemoryScope", "Per_Db",
                "--maxmemoryPolicy", "ALLKEYS-RANDOM",
                "--maxmemorySamples", "9",
                "--evictionTimeLimitMillis", "11",
                "--expireCleanupTimeLimitMillis", "13",
                "--nativeDefragEnabled",
                "--nativeDefragMaxMoveBytes", "1024",
                "--nativeDefragMaxObjects", "7",
                "--nativeDefragTimeLimitMillis", "3",
                "--nativeSlotCapacity", "2097152",
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
        Assert.assertEquals(65536, runtimeConfig.get("protocolMaxCommandBytes"));
        Assert.assertEquals(1048576L, runtimeConfig.get("protocolGlobalInFlightBytes"));
        Assert.assertEquals(1048576L, runtimeConfig.get("maxmemoryBytes"));
        Assert.assertEquals(YierdisServerRuntimeConfig.MaxmemoryScope.PER_DB, runtimeConfig.get("maxmemoryScope"));
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, runtimeConfig.get("maxmemoryPolicy"));
        Assert.assertEquals(9, runtimeConfig.get("maxmemorySamples"));
        Assert.assertEquals(11L, runtimeConfig.get("evictionTimeLimitMillis"));
        Assert.assertEquals(13L, runtimeConfig.get("expireCleanupTimeLimitMillis"));
        Assert.assertEquals(true, runtimeConfig.get("nativeDefragEnabled"));
        Assert.assertEquals(1024L, runtimeConfig.get("nativeDefragMaxMoveBytes"));
        Assert.assertEquals(7L, runtimeConfig.get("nativeDefragMaxObjects"));
        Assert.assertEquals(3L, runtimeConfig.get("nativeDefragTimeLimitMillis"));
        Assert.assertEquals(2_097_152, runtimeConfig.get("nativeSlotCapacity"));
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
    public void clientTimeoutAndOutputBufferArgsAreParsed() {
        YierdisServerArgs args = parse(
                "--client-idle-timeout-millis", "1000",
                "--client-output-buffer-limit-bytes", "2048",
                "--client-output-buffer-over-limit-millis", "3000"
        );
        YierdisServerRuntimeConfig config = args.toRuntimeConfig();
        Assert.assertEquals(1000, config.clientIdleTimeoutMillis());
        Assert.assertEquals(2048, config.clientOutputBufferLimitBytes());
        Assert.assertEquals(3000, config.clientOutputBufferOverLimitMillis());
    }

    @Test
    public void replyCapacityArgsRoundTripWithExactDefaultsAndRuntimeConfig() {
        YierdisServerArgs defaults = new YierdisServerArgs();
        defaults.normalizeAndValidate();
        YierdisServerRuntimeConfig defaultConfig = defaults.toRuntimeConfig();
        Assert.assertEquals(256L * 1024L * 1024L, defaultConfig.replyGlobalCapacityBytes());
        Assert.assertEquals(128L * 1024L * 1024L, defaultConfig.replyPerConnectionCapacityBytes());
        Assert.assertEquals(64L * 1024L * 1024L, defaultConfig.replyMaxTotalBytes());
        Assert.assertEquals(64 * 1024, defaultConfig.replyChunkPayloadBytes());
        Assert.assertEquals(4L * 1024L, defaultConfig.replyControlReservationBytes());
        Assert.assertEquals(5_000L, defaultConfig.replyDrainTimeoutMillis());

        YierdisServerArgs args = parse(
                "--replyGlobalCapacityBytes", "8192",
                "--replyPerConnectionCapacityBytes", "4096",
                "--replyMaxTotalBytes", "4096",
                "--replyChunkPayloadBytes", "128",
                "--replyControlReservationBytes", "1539",
                "--replyDrainTimeoutMillis", "17"
        );
        args.normalizeAndValidate();

        YierdisServerArgs copy = args.copy();
        Assert.assertEquals(args.toArgv(), copy.toArgv());
        YierdisServerArgs reparsed = parse(copy.toArgv().toArray(new String[0]));
        reparsed.normalizeAndValidate();
        Assert.assertEquals(copy.toRuntimeConfig(), reparsed.toRuntimeConfig());

        YierdisServerRuntimeConfig config = args.toRuntimeConfig();
        Assert.assertEquals(8192L, config.replyGlobalCapacityBytes());
        Assert.assertEquals(4096L, config.replyPerConnectionCapacityBytes());
        Assert.assertEquals(4096L, config.replyMaxTotalBytes());
        Assert.assertEquals(128, config.replyChunkPayloadBytes());
        Assert.assertEquals(1539L, config.replyControlReservationBytes());
        Assert.assertEquals(17L, config.replyDrainTimeoutMillis());
    }

    @Test
    public void replyCapacityArgsRejectInvalidIndividualAndRelativeLimits() {
        assertInvalidReplyConfig("--replyGlobalCapacityBytes", "0");
        assertInvalidReplyConfig("--replyGlobalCapacityBytes", "-1");
        assertInvalidReplyConfig("--replyPerConnectionCapacityBytes", "0");
        assertInvalidReplyConfig("--replyPerConnectionCapacityBytes", "-1");
        assertInvalidReplyConfig("--replyMaxTotalBytes", "0");
        assertInvalidReplyConfig("--replyMaxTotalBytes", "-1");
        assertInvalidReplyConfig("--replyChunkPayloadBytes", "0");
        assertInvalidReplyConfig("--replyChunkPayloadBytes", "-1");
        assertInvalidReplyConfig("--replyControlReservationBytes", "0");
        assertInvalidReplyConfig("--replyControlReservationBytes", "-1");
        assertInvalidReplyConfig(
                "--replyControlReservationBytes",
                Integer.toString(YierdisServerRuntimeConfig.REPLY_FIXED_OVERHEAD_BYTES)
        );
        assertInvalidReplyConfig("--replyControlReservationBytes", "1538");
        assertInvalidReplyConfig("--replyDrainTimeoutMillis", "0");
        assertInvalidReplyConfig("--replyDrainTimeoutMillis", "-1");
        assertInvalidReplyConfig(
                "--replyControlReservationBytes", "4097",
                "--replyMaxTotalBytes", "4096"
        );
        assertInvalidReplyConfig(
                "--replyMaxTotalBytes", "4097",
                "--replyPerConnectionCapacityBytes", "4096"
        );
        assertInvalidReplyConfig(
                "--replyPerConnectionCapacityBytes", "8193",
                "--replyGlobalCapacityBytes", "8192"
        );
        assertInvalidReplyConfig(
                "--replyChunkPayloadBytes", "65536",
                "--replyControlReservationBytes", "4096",
                "--replyMaxTotalBytes", Integer.toString(
                        65536 + 4096 + YierdisServerRuntimeConfig.REPLY_FIXED_OVERHEAD_BYTES - 1
                )
        );
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
    public void protocolLimitsRejectValuesAboveDecoderSafeMaximum() {
        YierdisServerArgs bulkArgs = parse("--protocolMaxBulkBytes", Integer.toString(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, bulkArgs::normalizeAndValidate);

        YierdisServerArgs argcArgs = parse("--protocolMaxArgs", Integer.toString(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, argcArgs::normalizeAndValidate);
    }

    @Test
    public void protocolCommandBytesParsesAndExportsToRuntimeConfig() {
        YierdisServerArgs args = parse("--protocolMaxCommandBytes", "1234");

        args.normalizeAndValidate();

        Assert.assertEquals(1234, args.protocolMaxCommandBytes);
        Assert.assertEquals(1234, args.copy().protocolMaxCommandBytes);
        Assert.assertTrue(args.toArgv().contains("--protocolMaxCommandBytes"));
        Assert.assertEquals(1234, args.toRuntimeConfig().protocolMaxCommandBytes());
    }

    @Test
    public void protocolGlobalInFlightBytesPreservesRawCliValueAndDerivesRuntimeDefault() {
        YierdisServerArgs explicit = parse(
                "--executorQueueMaxBytes", "67108864",
                "--protocolGlobalInFlightBytes", "1048576"
        );
        explicit.normalizeAndValidate();

        Assert.assertEquals(1048576L, explicit.protocolGlobalInFlightBytes);
        Assert.assertEquals(1048576L, explicit.copy().protocolGlobalInFlightBytes);
        Assert.assertTrue(explicit.toArgv().contains("--protocolGlobalInFlightBytes"));
        Assert.assertEquals(1048576L, explicit.toRuntimeConfig().protocolGlobalInFlightBytes());

        YierdisServerArgs minimum = parse("--executorQueueMaxBytes", "67108864");
        minimum.normalizeAndValidate();
        Assert.assertEquals(0L, minimum.protocolGlobalInFlightBytes);
        Assert.assertEquals(128L * 1024L * 1024L, minimum.toRuntimeConfig().protocolGlobalInFlightBytes());

        YierdisServerArgs doubledQueue = parse("--executorQueueMaxBytes", "83886080");
        doubledQueue.normalizeAndValidate();
        Assert.assertEquals(160L * 1024L * 1024L, doubledQueue.toRuntimeConfig().protocolGlobalInFlightBytes());

        YierdisServerArgs overflow = parse("--executorQueueMaxBytes", Long.toString(Long.MAX_VALUE));
        overflow.normalizeAndValidate();
        Assert.assertEquals(Long.MAX_VALUE, overflow.toRuntimeConfig().protocolGlobalInFlightBytes());

        YierdisServerArgs negative = parse("--protocolGlobalInFlightBytes", "-1");
        assertThrows(IllegalArgumentException.class, negative::normalizeAndValidate);
    }

    @Test
    public void nativeSlotCapacityParsesCopiesAndRoundTrips() {
        YierdisServerArgs args = parse("--nativeSlotCapacity", "2097152");

        args.normalizeAndValidate();

        Assert.assertEquals(2_097_152, args.nativeSlotCapacity);
        Assert.assertEquals(2_097_152, args.copy().nativeSlotCapacity);
        Assert.assertTrue(args.toArgv().contains("--nativeSlotCapacity"));
        Assert.assertEquals(2_097_152, args.toRuntimeConfig().nativeSlotCapacity());
    }

    @Test
    public void nativeSlotCapacityAllowsZeroAsDefaultSentinelAndRejectsNegativeValues() {
        YierdisServerArgs zero = parse("--nativeSlotCapacity", "0");
        zero.normalizeAndValidate();
        Assert.assertEquals(0, zero.nativeSlotCapacity);

        YierdisServerArgs negative = parse("--nativeSlotCapacity", "-1");
        assertThrows(IllegalArgumentException.class, negative::normalizeAndValidate);
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
        Assert.assertEquals(RespProtocolLimits.DEFAULT_MAX_BULK_BYTES, args.protocolMaxBulkBytes);
        Assert.assertEquals(RespProtocolLimits.DEFAULT_MAX_ARGS, args.protocolMaxArgs);
        Assert.assertEquals(RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES, args.protocolMaxLineBytes);
        Assert.assertEquals(RespProtocolLimits.DEFAULT_MAX_COMMAND_BYTES, args.protocolMaxCommandBytes);
    }

    private static YierdisServerArgs parse(String... argv) {
        YierdisServerArgs args = new YierdisServerArgs();
        new CommandLine(args).parseArgs(argv);
        return args;
    }

    private static void assertInvalidReplyConfig(String... argv) {
        YierdisServerArgs args = parse(argv);
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
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
