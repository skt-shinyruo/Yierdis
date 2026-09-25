package yier.bubu.redis.app.server.args;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.util.Properties;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;

public class YierdisServerFileConfigTest {
    @Test
    public void normalizeLowercasesSchedulingAndPolicy() {
        YierdisServerFileConfig args = parse("--executorSchedulingPolicy", "GLOBAL", "--maxmemoryPolicy", "ALLKEYS-LRU");
        args.normalizeAndValidate();
        Assert.assertEquals("global", args.executorSchedulingPolicy);
        Assert.assertEquals("allkeys-lru", args.maxmemoryPolicy);
    }

    @Test
    public void normalizedArgsConvertToRuntimeConfigWithoutLegacyOffheapFields() {
        YierdisServerFileConfig args = parse(
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
        Assert.assertEquals(1048576L, runtimeConfig.get("protocolGlobalInFlightBytes"));
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
        Assert.assertEquals(2_097_152, runtimeConfig.get("nativeSlotCapacity"));
        Assert.assertEquals(17L, runtimeConfig.get("keysTimeBudgetMillis"));
        Assert.assertEquals(23, runtimeConfig.get("keysMaxResults"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapBackend"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapMaxBytes"));
        Assert.assertFalse(runtimeConfig.containsKey("offheapKeysEnabled"));
    }

    @Test
    public void clientTimeoutAndOutputBufferArgsAreParsed() {
        YierdisServerFileConfig args = parse(
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
    public void replyCapacityArgsUseExactDefaultsAndRuntimeConfig() {
        YierdisServerFileConfig defaults = parse();
        defaults.normalizeAndValidate();
        YierdisServerRuntimeConfig defaultConfig = defaults.toRuntimeConfig();
        Assert.assertEquals(256L * 1024L * 1024L, defaultConfig.replyGlobalCapacityBytes());
        Assert.assertEquals(128L * 1024L * 1024L, defaultConfig.replyPerConnectionCapacityBytes());
        Assert.assertEquals(64L * 1024L * 1024L, defaultConfig.replyMaxTotalBytes());
        Assert.assertEquals(64 * 1024, defaultConfig.replyChunkPayloadBytes());
        Assert.assertEquals(4L * 1024L, defaultConfig.replyControlReservationBytes());
        Assert.assertEquals(5_000L, defaultConfig.replyDrainTimeoutMillis());

        YierdisServerFileConfig args = parse(
                "--replyGlobalCapacityBytes", "8192",
                "--replyPerConnectionCapacityBytes", "4096",
                "--replyMaxTotalBytes", "4096",
                "--replyChunkPayloadBytes", "128",
                "--replyControlReservationBytes", "1539",
                "--replyDrainTimeoutMillis", "17"
        );
        args.normalizeAndValidate();

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
        assertInvalidArgs("--replyGlobalCapacityBytes", "0");
        assertInvalidArgs("--replyGlobalCapacityBytes", "-1");
        assertInvalidArgs("--replyPerConnectionCapacityBytes", "0");
        assertInvalidArgs("--replyPerConnectionCapacityBytes", "-1");
        assertInvalidArgs("--replyMaxTotalBytes", "0");
        assertInvalidArgs("--replyMaxTotalBytes", "-1");
        assertInvalidArgs("--replyChunkPayloadBytes", "0");
        assertInvalidArgs("--replyChunkPayloadBytes", "-1");
        assertInvalidArgs("--replyControlReservationBytes", "0");
        assertInvalidArgs("--replyControlReservationBytes", "-1");
        assertInvalidArgs(
                "--replyControlReservationBytes",
                Integer.toString(YierdisServerRuntimeConfig.REPLY_FIXED_OVERHEAD_BYTES)
        );
        assertInvalidArgs("--replyControlReservationBytes", "1538");
        assertInvalidArgs("--replyDrainTimeoutMillis", "0");
        assertInvalidArgs("--replyDrainTimeoutMillis", "-1");
        assertInvalidArgs(
                "--replyControlReservationBytes", "4097",
                "--replyMaxTotalBytes", "4096"
        );
        assertInvalidArgs(
                "--replyMaxTotalBytes", "4097",
                "--replyPerConnectionCapacityBytes", "4096"
        );
        assertInvalidArgs(
                "--replyPerConnectionCapacityBytes", "8193",
                "--replyGlobalCapacityBytes", "8192"
        );
        assertInvalidArgs(
                "--replyChunkPayloadBytes", "65536",
                "--replyControlReservationBytes", "4096",
                "--replyMaxTotalBytes", Integer.toString(
                        65536 + 4096 + YierdisServerRuntimeConfig.REPLY_FIXED_OVERHEAD_BYTES - 1
                )
        );
    }

    @Test
    public void invalidPortIsRejected() {
        YierdisServerFileConfig args = parse("--port", "-1");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidWatermarkOrderIsRejected() {
        YierdisServerFileConfig args = parse("--backpressureHigh", "10", "--backpressureLow", "10");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void invalidBytesWatermarkOrderIsRejected() {
        YierdisServerFileConfig args = parse("--backpressureBytesHigh", "10", "--backpressureBytesLow", "10");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void bytesLowWithoutBytesHighIsRejected() {
        YierdisServerFileConfig args = parse("--backpressureBytesHigh", "0", "--backpressureBytesLow", "1");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void executorLimitArgsRejectInvalidValues() {
        assertInvalidArgs("--executorQueueCapacity", "0");
        assertInvalidArgs("--executorQueueCapacity", "-1");
        assertInvalidArgs("--executorQueueMaxBytes", "-1");
        assertInvalidArgs("--backpressureHigh", "0");
        assertInvalidArgs("--backpressureHigh", "-1");
        assertInvalidArgs("--backpressureLow", "-1");
        assertInvalidArgs("--backpressureHigh", "10", "--backpressureLow", "11");
        assertInvalidArgs("--backpressureBytesHigh", "-1");
        assertInvalidArgs("--backpressureBytesLow", "-1");
        assertInvalidArgs("--backpressureBytesHigh", "10", "--backpressureBytesLow", "11");
        assertInvalidArgs("--executorMaxDrain", "0");
        assertInvalidArgs("--executorMaxDrain", "-1");
        assertInvalidArgs("--executorDrainMillis", "0");
        assertInvalidArgs("--executorDrainMillis", "-1");
    }

    @Test
    public void deletedOffheapFlagsAreRejectedAtParseTime() {
        assertThrows(IllegalArgumentException.class, () -> parse("--offheapBackend", "foreign"));
        assertThrows(IllegalArgumentException.class, () -> parse("--offheapMaxBytes", "1"));
        assertThrows(IllegalArgumentException.class, () -> parse("--offheapKeysEnabled", "true"));
    }

    @Test
    public void invalidMaxmemoryPolicyIsRejected() {
        YierdisServerFileConfig args = parse("--maxmemoryPolicy", "random-evict");
        assertThrows(IllegalArgumentException.class, args::normalizeAndValidate);
    }

    @Test
    public void protocolLimitsRejectValuesAboveDecoderSafeMaximum() {
        YierdisServerFileConfig bulkArgs = parse("--protocolMaxBulkBytes", Integer.toString(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, bulkArgs::normalizeAndValidate);

        YierdisServerFileConfig argcArgs = parse("--protocolMaxArgs", Integer.toString(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, argcArgs::normalizeAndValidate);
    }

    @Test
    public void protocolCommandBytesParsesAndExportsToRuntimeConfig() {
        YierdisServerFileConfig args = parse("--protocolMaxCommandBytes", "1234");

        args.normalizeAndValidate();

        Assert.assertEquals(1234, args.protocolMaxCommandBytes);
        Assert.assertEquals(1234, args.toRuntimeConfig().protocolMaxCommandBytes());
    }

    @Test
    public void protocolGlobalInFlightBytesPreservesRawCliValueAndDerivesRuntimeDefault() {
        YierdisServerFileConfig explicit = parse(
                "--executorQueueMaxBytes", "67108864",
                "--protocolGlobalInFlightBytes", "1048576"
        );
        explicit.normalizeAndValidate();

        Assert.assertEquals(1048576L, explicit.protocolGlobalInFlightBytes);
        Assert.assertEquals(1048576L, explicit.toRuntimeConfig().protocolGlobalInFlightBytes());

        YierdisServerFileConfig minimum = parse("--executorQueueMaxBytes", "67108864");
        minimum.normalizeAndValidate();
        Assert.assertEquals(0L, minimum.protocolGlobalInFlightBytes);
        Assert.assertEquals(128L * 1024L * 1024L, minimum.toRuntimeConfig().protocolGlobalInFlightBytes());

        YierdisServerFileConfig doubledQueue = parse("--executorQueueMaxBytes", "83886080");
        doubledQueue.normalizeAndValidate();
        Assert.assertEquals(160L * 1024L * 1024L, doubledQueue.toRuntimeConfig().protocolGlobalInFlightBytes());

        YierdisServerFileConfig overflow = parse("--executorQueueMaxBytes", Long.toString(Long.MAX_VALUE));
        overflow.normalizeAndValidate();
        Assert.assertEquals(Long.MAX_VALUE, overflow.toRuntimeConfig().protocolGlobalInFlightBytes());

        YierdisServerFileConfig negative = parse("--protocolGlobalInFlightBytes", "-1");
        assertThrows(IllegalArgumentException.class, negative::normalizeAndValidate);
    }

    @Test
    public void nativeSlotCapacityParsesAndExportsToRuntimeConfig() {
        YierdisServerFileConfig args = parse("--nativeSlotCapacity", "2097152");

        args.normalizeAndValidate();

        Assert.assertEquals(2_097_152, args.nativeSlotCapacity);
        Assert.assertEquals(2_097_152, args.toRuntimeConfig().nativeSlotCapacity());
    }

    @Test
    public void nativeSlotCapacityAllowsZeroAsDefaultSentinelAndRejectsNegativeValues() {
        YierdisServerFileConfig zero = parse("--nativeSlotCapacity", "0");
        zero.normalizeAndValidate();
        Assert.assertEquals(0, zero.nativeSlotCapacity);

        YierdisServerFileConfig negative = parse("--nativeSlotCapacity", "-1");
        assertThrows(IllegalArgumentException.class, negative::normalizeAndValidate);
    }

    @Test
    public void normalizeAcceptsCorePolicyUnderscoreAliases() {
        YierdisServerFileConfig args = parse("--maxmemoryPolicy", "ALLKEYS_RANDOM");

        args.normalizeAndValidate();

        Assert.assertEquals("allkeys-random", args.maxmemoryPolicy);
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, args.toRuntimeConfig().maxmemoryPolicy());
    }

    @Test
    public void protocolDefaultsMatchProtocolLimitsSsot() {
        YierdisServerFileConfig args = parse();
        Assert.assertEquals(RespProtocolLimits.DEFAULT_MAX_BULK_BYTES, args.protocolMaxBulkBytes);
        Assert.assertEquals(RespProtocolLimits.DEFAULT_MAX_ARGS, args.protocolMaxArgs);
        Assert.assertEquals(RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES, args.protocolMaxLineBytes);
        Assert.assertEquals(RespProtocolLimits.DEFAULT_MAX_COMMAND_BYTES, args.protocolMaxCommandBytes);
    }

    private static YierdisServerFileConfig parse(String... kv) {
        Properties props = new Properties();
        for (int i = 0; i < kv.length; i++) {
            String key = kv[i];
            if (key.startsWith("--")) {
                key = key.substring(2);
            }
            if (key.equals("noCleanup") || key.equals("nativeDefragEnabled")) {
                props.setProperty(key, "true");
            } else {
                props.setProperty(key, kv[++i]);
            }
        }
        return YierdisServerFileConfig.fromProperties(props);
    }

    private static void assertInvalidArgs(String... argv) {
        YierdisServerFileConfig args = parse(argv);
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
