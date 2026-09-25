package yier.bubu.redis.app.server.args;

import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.LinkedHashSet;
import java.util.Set;

public final class YierdisServerArgs {
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

    private final Set<String> specifiedOptions = new LinkedHashSet<>();

    private SchedulingPolicy parsedExecutorSchedulingPolicy;
    private YierdisInstanceConfig.MaxmemoryScope parsedMaxmemoryScope;
    private MaxmemoryPolicy parsedMaxmemoryPolicy;
    private YierdisServerRuntimeConfig cachedRuntimeConfig;

    /**
     * 手写 argv 解析（无 picocli）：只认识 assign(...) 里列出的 --选项与两个 flag，其余一律报错。
     * 支持 "--name value" 与 "--name=value" 两种写法；未知选项与位置参数都是错误。
     */
    public static YierdisServerArgs parse(String... argv) {
        YierdisServerArgs args = new YierdisServerArgs();
        for (int i = 0; i < argv.length; i++) {
            String name = argv[i];
            String value = null;
            if (name.startsWith("--")) {
                int eq = name.indexOf('=');
                if (eq >= 0) {
                    value = name.substring(eq + 1);
                    name = name.substring(0, eq);
                }
            }
            switch (name) {
                case "--noCleanup":
                    rejectInlineValue(name, value);
                    args.noCleanup = true;
                    break;
                case "--nativeDefragEnabled":
                    rejectInlineValue(name, value);
                    args.nativeDefragEnabled = true;
                    break;
                default: {
                    if (!isValueOption(name)) {
                        throw unknown(name, i);
                    }
                    if (value == null) {
                        if (++i >= argv.length) {
                            throw new IllegalArgumentException("Missing required parameter for option '" + name + "'");
                        }
                        value = argv[i];
                    }
                    assign(args, name, value);
                }
            }
            args.specifiedOptions.add(name);
        }
        return args;
    }

    /** 解析时是否显式给了某个选项（用于强制要求 --maxmemoryBytes 的检查）。 */
    public boolean wasSpecified(String name) {
        return specifiedOptions.contains(name);
    }

    private static boolean isValueOption(String name) {
        return switch (name) {
            case "--bind", "--port", "--maxClients", "--databases", "--cleanupIntervalMillis",
                 "--ioThreads", "--executorQueueCapacity", "--executorQueueMaxBytes",
                 "--executorSchedulingPolicy", "--backpressureHigh", "--backpressureLow",
                 "--backpressureBytesHigh", "--backpressureBytesLow", "--executorMaxDrain",
                 "--executorDrainMillis", "--transactionQueueMaxCommands", "--transactionQueueMaxBytes",
                 "--protocolMaxBulkBytes", "--protocolMaxArgs", "--protocolMaxLineBytes",
                 "--protocolMaxCommandBytes", "--protocolGlobalInFlightBytes",
                 "--client-idle-timeout-millis", "--client-output-buffer-limit-bytes",
                 "--client-output-buffer-over-limit-millis",
                 "--replyGlobalCapacityBytes", "--replyPerConnectionCapacityBytes", "--replyMaxTotalBytes",
                 "--replyChunkPayloadBytes", "--replyControlReservationBytes", "--replyDrainTimeoutMillis",
                 "--maxmemoryBytes", "--maxmemoryScope", "--maxmemoryPolicy", "--maxmemorySamples",
                 "--evictionTimeLimitMillis", "--expireCleanupTimeLimitMillis",
                 "--nativeDefragMaxMoveBytes", "--nativeDefragMaxObjects", "--nativeDefragTimeLimitMillis",
                 "--nativeSlotCapacity", "--keysTimeBudgetMillis", "--keysMaxResults" -> true;
            default -> false;
        };
    }

    private static void assign(YierdisServerArgs args, String name, String raw) {
        switch (name) {
            case "--bind" -> args.bind = raw;
            case "--port" -> args.port = intValue(name, raw);
            case "--maxClients" -> args.maxClients = intValue(name, raw);
            case "--databases" -> args.databases = intValue(name, raw);
            case "--cleanupIntervalMillis" -> args.cleanupIntervalMillis = longValue(name, raw);
            case "--ioThreads" -> args.ioThreads = intValue(name, raw);
            case "--executorQueueCapacity" -> args.executorQueueCapacity = intValue(name, raw);
            case "--executorQueueMaxBytes" -> args.executorQueueMaxBytes = longValue(name, raw);
            case "--executorSchedulingPolicy" -> args.executorSchedulingPolicy = raw;
            case "--backpressureHigh" -> args.backpressureHighWatermark = intValue(name, raw);
            case "--backpressureLow" -> args.backpressureLowWatermark = intValue(name, raw);
            case "--backpressureBytesHigh" -> args.backpressureBytesHighWatermark = longValue(name, raw);
            case "--backpressureBytesLow" -> args.backpressureBytesLowWatermark = longValue(name, raw);
            case "--executorMaxDrain" -> args.executorMaxDrainCommands = intValue(name, raw);
            case "--executorDrainMillis" -> args.executorDrainTimeLimitMillis = longValue(name, raw);
            case "--transactionQueueMaxCommands" -> args.transactionQueueMaxCommands = intValue(name, raw);
            case "--transactionQueueMaxBytes" -> args.transactionQueueMaxBytes = longValue(name, raw);
            case "--protocolMaxBulkBytes" -> args.protocolMaxBulkBytes = intValue(name, raw);
            case "--protocolMaxArgs" -> args.protocolMaxArgs = intValue(name, raw);
            case "--protocolMaxLineBytes" -> args.protocolMaxLineBytes = intValue(name, raw);
            case "--protocolMaxCommandBytes" -> args.protocolMaxCommandBytes = intValue(name, raw);
            case "--protocolGlobalInFlightBytes" -> args.protocolGlobalInFlightBytes = longValue(name, raw);
            case "--client-idle-timeout-millis" -> args.clientIdleTimeoutMillis = longValue(name, raw);
            case "--client-output-buffer-limit-bytes" -> args.clientOutputBufferLimitBytes = longValue(name, raw);
            case "--client-output-buffer-over-limit-millis" -> args.clientOutputBufferOverLimitMillis = longValue(name, raw);
            case "--replyGlobalCapacityBytes" -> args.replyGlobalCapacityBytes = longValue(name, raw);
            case "--replyPerConnectionCapacityBytes" -> args.replyPerConnectionCapacityBytes = longValue(name, raw);
            case "--replyMaxTotalBytes" -> args.replyMaxTotalBytes = longValue(name, raw);
            case "--replyChunkPayloadBytes" -> args.replyChunkPayloadBytes = intValue(name, raw);
            case "--replyControlReservationBytes" -> args.replyControlReservationBytes = longValue(name, raw);
            case "--replyDrainTimeoutMillis" -> args.replyDrainTimeoutMillis = longValue(name, raw);
            case "--maxmemoryBytes" -> args.maxmemoryBytes = longValue(name, raw);
            case "--maxmemoryScope" -> args.maxmemoryScope = raw;
            case "--maxmemoryPolicy" -> args.maxmemoryPolicy = raw;
            case "--maxmemorySamples" -> args.maxmemorySamples = intValue(name, raw);
            case "--evictionTimeLimitMillis" -> args.evictionTimeLimitMillis = longValue(name, raw);
            case "--expireCleanupTimeLimitMillis" -> args.expireCleanupTimeLimitMillis = longValue(name, raw);
            case "--nativeDefragMaxMoveBytes" -> args.nativeDefragMaxMoveBytes = longValue(name, raw);
            case "--nativeDefragMaxObjects" -> args.nativeDefragMaxObjects = longValue(name, raw);
            case "--nativeDefragTimeLimitMillis" -> args.nativeDefragTimeLimitMillis = longValue(name, raw);
            case "--nativeSlotCapacity" -> args.nativeSlotCapacity = intValue(name, raw);
            case "--keysTimeBudgetMillis" -> args.keysTimeBudgetMillis = longValue(name, raw);
            case "--keysMaxResults" -> args.keysMaxResults = intValue(name, raw);
            default -> throw new IllegalArgumentException("Unknown option: '" + name + "'");
        }
    }

    private static void rejectInlineValue(String name, String inlineValue) {
        if (inlineValue != null) {
            throw new IllegalArgumentException("option '" + name + "' does not take a value");
        }
    }

    private static IllegalArgumentException unknown(String name, int index) {
        if (name.startsWith("--")) {
            return new IllegalArgumentException("Unknown option: '" + name + "'");
        }
        return new IllegalArgumentException("Unmatched argument at index " + index + ": '" + name + "'");
    }

    private static int intValue(String name, String raw) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for option '" + name + "': '" + raw + "' is not an int");
        }
    }

    private static long longValue(String name, String raw) {
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for option '" + name + "': '" + raw + "' is not a long");
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
