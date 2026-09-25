package yier.bubu.redis.app.server.args;

import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

public final class YierdisServerFileConfig {
    private static final int DEFAULT_PROTOCOL_MAX_BULK_BYTES = RespProtocolLimits.DEFAULT_MAX_BULK_BYTES;
    private static final int DEFAULT_PROTOCOL_MAX_ARGS = RespProtocolLimits.DEFAULT_MAX_ARGS;
    private static final int DEFAULT_PROTOCOL_MAX_LINE_BYTES = RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES;
    private static final int DEFAULT_PROTOCOL_MAX_COMMAND_BYTES = RespProtocolLimits.DEFAULT_MAX_COMMAND_BYTES;
    private static final long MIN_PROTOCOL_GLOBAL_IN_FLIGHT_BYTES = 128L * 1024L * 1024L;

    private static final long DEFAULT_EXECUTOR_QUEUE_MAX_BYTES = 64L * 1024 * 1024; // 64 MiB
    private static final int DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS = 1024;
    private static final long DEFAULT_TRANSACTION_QUEUE_MAX_BYTES = DEFAULT_EXECUTOR_QUEUE_MAX_BYTES;
    private static final long DEFAULT_BACKPRESSURE_BYTES_HIGH = 16L * 1024 * 1024; // 16 MiB
    private static final long DEFAULT_BACKPRESSURE_BYTES_LOW = 8L * 1024 * 1024; // 8 MiB
    private static final long DEFAULT_REPLY_GLOBAL_CAPACITY_BYTES = 256L * 1024L * 1024L;
    private static final long DEFAULT_REPLY_PER_CONNECTION_CAPACITY_BYTES = 128L * 1024L * 1024L;
    private static final long DEFAULT_REPLY_MAX_TOTAL_BYTES = 64L * 1024L * 1024L;
    private static final int DEFAULT_REPLY_CHUNK_PAYLOAD_BYTES = 64 * 1024;
    private static final long DEFAULT_REPLY_CONTROL_RESERVATION_BYTES = 4L * 1024L;
    private static final long DEFAULT_REPLY_DRAIN_TIMEOUT_MILLIS = 5_000L;

    public String bind = "127.0.0.1";

    public int port = 6378;

    public int maxClients = 1024;

    public int databases = 16;

    public long cleanupIntervalMillis = 1000;

    public boolean noCleanup;

    public int ioThreads = 1;

    public int executorQueueCapacity = 1024;

    public long executorQueueMaxBytes = DEFAULT_EXECUTOR_QUEUE_MAX_BYTES;

    public String executorSchedulingPolicy = "fair";

    public int backpressureHighWatermark = 256;

    public int backpressureLowWatermark = 128;

    public long backpressureBytesHighWatermark = DEFAULT_BACKPRESSURE_BYTES_HIGH;

    public long backpressureBytesLowWatermark = DEFAULT_BACKPRESSURE_BYTES_LOW;

    public int executorMaxDrainCommands = 512;

    public long executorDrainTimeLimitMillis = 2;

    public int transactionQueueMaxCommands = DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS;

    public long transactionQueueMaxBytes = DEFAULT_TRANSACTION_QUEUE_MAX_BYTES;

    public int protocolMaxBulkBytes = DEFAULT_PROTOCOL_MAX_BULK_BYTES;

    public int protocolMaxArgs = DEFAULT_PROTOCOL_MAX_ARGS;

    public int protocolMaxLineBytes = DEFAULT_PROTOCOL_MAX_LINE_BYTES;

    public int protocolMaxCommandBytes = DEFAULT_PROTOCOL_MAX_COMMAND_BYTES;

    public long protocolGlobalInFlightBytes;

    public long clientIdleTimeoutMillis = 0;

    public long clientOutputBufferLimitBytes = 67108864;

    public long clientOutputBufferOverLimitMillis = 10000;

    public long replyGlobalCapacityBytes = DEFAULT_REPLY_GLOBAL_CAPACITY_BYTES;

    public long replyPerConnectionCapacityBytes = DEFAULT_REPLY_PER_CONNECTION_CAPACITY_BYTES;

    public long replyMaxTotalBytes = DEFAULT_REPLY_MAX_TOTAL_BYTES;

    public int replyChunkPayloadBytes = DEFAULT_REPLY_CHUNK_PAYLOAD_BYTES;

    public long replyControlReservationBytes = DEFAULT_REPLY_CONTROL_RESERVATION_BYTES;

    public long replyDrainTimeoutMillis = DEFAULT_REPLY_DRAIN_TIMEOUT_MILLIS;

    public long maxmemoryBytes = 0;

    public String maxmemoryScope = "global";

    public String maxmemoryPolicy = "noeviction";

    public int maxmemorySamples = 5;

    public long evictionTimeLimitMillis = 5;

    public long expireCleanupTimeLimitMillis = 5;

    public boolean nativeDefragEnabled;

    public long nativeDefragMaxMoveBytes = 64L * 1024L;

    public long nativeDefragMaxObjects = 64L;

    public long nativeDefragTimeLimitMillis = 1L;

    public int nativeSlotCapacity;

    public long keysTimeBudgetMillis = 0;

    public int keysMaxResults = Integer.MAX_VALUE;

    private final Set<String> specifiedKeys = new LinkedHashSet<>();

    private SchedulingPolicy parsedExecutorSchedulingPolicy;
    private YierdisInstanceConfig.MaxmemoryScope parsedMaxmemoryScope;
    private MaxmemoryPolicy parsedMaxmemoryPolicy;
    private YierdisServerRuntimeConfig cachedRuntimeConfig;

    /**
     * 从 Properties 构造配置（启动配置文件的唯一来源）。键名不带 "--" 前缀；
     * 未知键一律报错（拼错的键静默失效是线上事故的经典来源），缺省键取字段默认值。
     * 布尔项（noCleanup、nativeDefragEnabled）的值为 "true"/"false"，空值按 true 处理。
     */
    public static YierdisServerFileConfig fromProperties(Properties props) {
        YierdisServerFileConfig config = new YierdisServerFileConfig();
        for (String name : props.stringPropertyNames()) {
            String value = props.getProperty(name);
            switch (name) {
                case "noCleanup" -> config.noCleanup = booleanValue(name, value);
                case "nativeDefragEnabled" -> config.nativeDefragEnabled = booleanValue(name, value);
                default -> {
                    if (!isValueKey(name)) {
                        throw new IllegalArgumentException("Unknown configuration key: '" + name + "'");
                    }
                    assign(config, name, value);
                }
            }
            config.specifiedKeys.add(name);
        }
        return config;
    }

    /** 配置里是否显式给了某个键（用于强制要求 maxmemoryBytes 的检查）。 */
    public boolean wasSpecified(String key) {
        return specifiedKeys.contains(key);
    }

    private static boolean isValueKey(String name) {
        return switch (name) {
            case "bind", "port", "maxClients", "databases", "cleanupIntervalMillis",
                 "ioThreads", "executorQueueCapacity", "executorQueueMaxBytes",
                 "executorSchedulingPolicy", "backpressureHigh", "backpressureLow",
                 "backpressureBytesHigh", "backpressureBytesLow", "executorMaxDrain",
                 "executorDrainMillis", "transactionQueueMaxCommands", "transactionQueueMaxBytes",
                 "protocolMaxBulkBytes", "protocolMaxArgs", "protocolMaxLineBytes",
                 "protocolMaxCommandBytes", "protocolGlobalInFlightBytes",
                 "client-idle-timeout-millis", "client-output-buffer-limit-bytes",
                 "client-output-buffer-over-limit-millis",
                 "replyGlobalCapacityBytes", "replyPerConnectionCapacityBytes", "replyMaxTotalBytes",
                 "replyChunkPayloadBytes", "replyControlReservationBytes", "replyDrainTimeoutMillis",
                 "maxmemoryBytes", "maxmemoryScope", "maxmemoryPolicy", "maxmemorySamples",
                 "evictionTimeLimitMillis", "expireCleanupTimeLimitMillis",
                 "nativeDefragMaxMoveBytes", "nativeDefragMaxObjects", "nativeDefragTimeLimitMillis",
                 "nativeSlotCapacity", "keysTimeBudgetMillis", "keysMaxResults" -> true;
            default -> false;
        };
    }

    private static void assign(YierdisServerFileConfig config, String name, String raw) {
        switch (name) {
            case "bind" -> config.bind = raw;
            case "port" -> config.port = intValue(name, raw);
            case "maxClients" -> config.maxClients = intValue(name, raw);
            case "databases" -> config.databases = intValue(name, raw);
            case "cleanupIntervalMillis" -> config.cleanupIntervalMillis = longValue(name, raw);
            case "ioThreads" -> config.ioThreads = intValue(name, raw);
            case "executorQueueCapacity" -> config.executorQueueCapacity = intValue(name, raw);
            case "executorQueueMaxBytes" -> config.executorQueueMaxBytes = longValue(name, raw);
            case "executorSchedulingPolicy" -> config.executorSchedulingPolicy = raw;
            case "backpressureHigh" -> config.backpressureHighWatermark = intValue(name, raw);
            case "backpressureLow" -> config.backpressureLowWatermark = intValue(name, raw);
            case "backpressureBytesHigh" -> config.backpressureBytesHighWatermark = longValue(name, raw);
            case "backpressureBytesLow" -> config.backpressureBytesLowWatermark = longValue(name, raw);
            case "executorMaxDrain" -> config.executorMaxDrainCommands = intValue(name, raw);
            case "executorDrainMillis" -> config.executorDrainTimeLimitMillis = longValue(name, raw);
            case "transactionQueueMaxCommands" -> config.transactionQueueMaxCommands = intValue(name, raw);
            case "transactionQueueMaxBytes" -> config.transactionQueueMaxBytes = longValue(name, raw);
            case "protocolMaxBulkBytes" -> config.protocolMaxBulkBytes = intValue(name, raw);
            case "protocolMaxArgs" -> config.protocolMaxArgs = intValue(name, raw);
            case "protocolMaxLineBytes" -> config.protocolMaxLineBytes = intValue(name, raw);
            case "protocolMaxCommandBytes" -> config.protocolMaxCommandBytes = intValue(name, raw);
            case "protocolGlobalInFlightBytes" -> config.protocolGlobalInFlightBytes = longValue(name, raw);
            case "client-idle-timeout-millis" -> config.clientIdleTimeoutMillis = longValue(name, raw);
            case "client-output-buffer-limit-bytes" -> config.clientOutputBufferLimitBytes = longValue(name, raw);
            case "client-output-buffer-over-limit-millis" -> config.clientOutputBufferOverLimitMillis = longValue(name, raw);
            case "replyGlobalCapacityBytes" -> config.replyGlobalCapacityBytes = longValue(name, raw);
            case "replyPerConnectionCapacityBytes" -> config.replyPerConnectionCapacityBytes = longValue(name, raw);
            case "replyMaxTotalBytes" -> config.replyMaxTotalBytes = longValue(name, raw);
            case "replyChunkPayloadBytes" -> config.replyChunkPayloadBytes = intValue(name, raw);
            case "replyControlReservationBytes" -> config.replyControlReservationBytes = longValue(name, raw);
            case "replyDrainTimeoutMillis" -> config.replyDrainTimeoutMillis = longValue(name, raw);
            case "maxmemoryBytes" -> config.maxmemoryBytes = longValue(name, raw);
            case "maxmemoryScope" -> config.maxmemoryScope = raw;
            case "maxmemoryPolicy" -> config.maxmemoryPolicy = raw;
            case "maxmemorySamples" -> config.maxmemorySamples = intValue(name, raw);
            case "evictionTimeLimitMillis" -> config.evictionTimeLimitMillis = longValue(name, raw);
            case "expireCleanupTimeLimitMillis" -> config.expireCleanupTimeLimitMillis = longValue(name, raw);
            case "nativeDefragMaxMoveBytes" -> config.nativeDefragMaxMoveBytes = longValue(name, raw);
            case "nativeDefragMaxObjects" -> config.nativeDefragMaxObjects = longValue(name, raw);
            case "nativeDefragTimeLimitMillis" -> config.nativeDefragTimeLimitMillis = longValue(name, raw);
            case "nativeSlotCapacity" -> config.nativeSlotCapacity = intValue(name, raw);
            case "keysTimeBudgetMillis" -> config.keysTimeBudgetMillis = longValue(name, raw);
            case "keysMaxResults" -> config.keysMaxResults = intValue(name, raw);
            default -> throw new IllegalArgumentException("Unknown configuration key: '" + name + "'");
        }
    }

    private static boolean booleanValue(String name, String raw) {
        if (raw == null || raw.isBlank()) {
            return true;
        }
        String normalized = raw.trim();
        if (normalized.equalsIgnoreCase("true")) {
            return true;
        }
        if (normalized.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException("Invalid value for key '" + name + "': '" + raw + "' is not a boolean");
    }

    private static int intValue(String name, String raw) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for key '" + name + "': '" + raw + "' is not an int");
        }
    }

    private static long longValue(String name, String raw) {
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for key '" + name + "': '" + raw + "' is not a long");
        }
    }

    public void normalizeAndValidate() {
        if (cachedRuntimeConfig != null) {
            return;
        }
        if (noCleanup) {
            cleanupIntervalMillis = 0;
        }
        if (bind != null) {
            bind = bind.trim();
        }
        parsedExecutorSchedulingPolicy = parseExecutorSchedulingPolicy(executorSchedulingPolicy);
        executorSchedulingPolicy = parsedExecutorSchedulingPolicy.name().toLowerCase(java.util.Locale.ROOT);
        parsedMaxmemoryScope = YierdisServerRuntimeConfig.parseMaxmemoryScope(maxmemoryScope);
        maxmemoryScope = parsedMaxmemoryScope == YierdisInstanceConfig.MaxmemoryScope.PER_DB ? "per-db" : "global";
        parsedMaxmemoryPolicy = MaxmemoryPolicy.parse(maxmemoryPolicy);
        maxmemoryPolicy = parsedMaxmemoryPolicy.redisName();
        cachedRuntimeConfig = buildRuntimeConfig();
    }

    /**
     * Convert normalized CLI args into the canonical runtime config.
     * <p>
     * The first call runs {@link #normalizeAndValidate()}, which parses each enum once and builds
     * the validated record; the cached instance is returned on subsequent calls.
     */
    public YierdisServerRuntimeConfig toRuntimeConfig() {
        if (cachedRuntimeConfig == null) {
            normalizeAndValidate();
        }
        return cachedRuntimeConfig;
    }

    private YierdisServerRuntimeConfig buildRuntimeConfig() {
        return new YierdisServerRuntimeConfig(
                bind,
                port,
                maxClients,
                databases,
                cleanupIntervalMillis,
                ioThreads,
                executorQueueCapacity,
                executorQueueMaxBytes,
                parsedExecutorSchedulingPolicy,
                backpressureHighWatermark,
                backpressureLowWatermark,
                backpressureBytesHighWatermark,
                backpressureBytesLowWatermark,
                executorMaxDrainCommands,
                executorDrainTimeLimitMillis,
                transactionQueueMaxCommands,
                transactionQueueMaxBytes,
                protocolMaxBulkBytes,
                protocolMaxArgs,
                protocolMaxLineBytes,
                protocolMaxCommandBytes,
                clientIdleTimeoutMillis,
                clientOutputBufferLimitBytes,
                clientOutputBufferOverLimitMillis,
                replyGlobalCapacityBytes,
                replyPerConnectionCapacityBytes,
                replyMaxTotalBytes,
                replyChunkPayloadBytes,
                replyControlReservationBytes,
                replyDrainTimeoutMillis,
                maxmemoryBytes,
                parsedMaxmemoryScope,
                parsedMaxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragEnabled,
                nativeDefragMaxMoveBytes,
                nativeDefragMaxObjects,
                nativeDefragTimeLimitMillis,
                nativeSlotCapacity,
                keysTimeBudgetMillis,
                keysMaxResults,
                deriveProtocolGlobalInFlightBytes(executorQueueMaxBytes, protocolGlobalInFlightBytes)
        );
    }

    private static SchedulingPolicy parseExecutorSchedulingPolicy(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("executorSchedulingPolicy must not be blank");
        }
        try {
            return SchedulingPolicy.valueOf(
                    rawValue.trim().toUpperCase(java.util.Locale.ROOT)
            );
        } catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException("unsupported executorSchedulingPolicy: " + rawValue);
        }
    }

    private static long deriveProtocolGlobalInFlightBytes(long executorQueueMaxBytes, long configuredBytes) {
        if (configuredBytes < 0L) {
            throw new IllegalArgumentException("protocolGlobalInFlightBytes must be >= 0");
        }
        if (configuredBytes > 0L) {
            return configuredBytes;
        }
        long queueBytes = Math.max(0L, executorQueueMaxBytes);
        long doubled = queueBytes > Long.MAX_VALUE / 2L ? Long.MAX_VALUE : queueBytes * 2L;
        return Math.max(MIN_PROTOCOL_GLOBAL_IN_FLIGHT_BYTES, doubled);
    }

}
