package yier.bubu.redis.app.server.args;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

@Command(
        name = "yierdis",
        description = "A simplified Redis RESP server (teaching-oriented).",
        sortOptions = false,
        usageHelpAutoWidth = true
)
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

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    public boolean help;

    @Option(names = "--bind", defaultValue = "127.0.0.1", description = "TCP host or address to bind.")
    public String bind = "127.0.0.1";

    @Option(names = "--port", defaultValue = "6378", description = "TCP port to bind.")
    public int port = 6378;

    @Option(names = "--maxClients", defaultValue = "1024", description = "Maximum accepted client connections.")
    public int maxClients = 1024;

    @Option(
            names = "--databases",
            defaultValue = "16",
            description = "Number of logical databases (SELECT 0..N-1)."
    )
    public int databases = 16;

    @Option(
            names = "--cleanupIntervalMillis",
            defaultValue = "1000",
            description = "Expiration cleanup interval in milliseconds (0 disables cleanup)."
    )
    public long cleanupIntervalMillis = 1000;

    @Option(names = "--noCleanup", description = "Disable periodic expiration cleanup.")
    public boolean noCleanup;

    @Option(names = "--ioThreads", defaultValue = "1", description = "Netty I/O threads.")
    public int ioThreads = 1;

    @Option(
            names = "--executorQueueCapacity",
            defaultValue = "1024",
            description = "Command executor queue capacity."
    )
    public int executorQueueCapacity = 1024;

    @Option(
            names = "--executorQueueMaxBytes",
            defaultValue = "" + DEFAULT_EXECUTOR_QUEUE_MAX_BYTES,
            description = "Command executor queue max bytes (0 disables)."
    )
    public long executorQueueMaxBytes = DEFAULT_EXECUTOR_QUEUE_MAX_BYTES;

    @Option(
            names = "--executorSchedulingPolicy",
            defaultValue = "fair",
            description = "Executor scheduling policy: global|fair."
    )
    public String executorSchedulingPolicy = "fair";

    @Option(names = "--backpressureHigh", defaultValue = "256", description = "Backpressure high watermark.")
    public int backpressureHighWatermark = 256;

    @Option(names = "--backpressureLow", defaultValue = "128", description = "Backpressure low watermark.")
    public int backpressureLowWatermark = 128;

    @Option(
            names = "--backpressureBytesHigh",
            defaultValue = "" + DEFAULT_BACKPRESSURE_BYTES_HIGH,
            description = "Backpressure bytes high watermark (0 disables)."
    )
    public long backpressureBytesHighWatermark = DEFAULT_BACKPRESSURE_BYTES_HIGH;

    @Option(
            names = "--backpressureBytesLow",
            defaultValue = "" + DEFAULT_BACKPRESSURE_BYTES_LOW,
            description = "Backpressure bytes low watermark (0 disables)."
    )
    public long backpressureBytesLowWatermark = DEFAULT_BACKPRESSURE_BYTES_LOW;

    @Option(names = "--executorMaxDrain", defaultValue = "512", description = "Max commands drained per executor tick.")
    public int executorMaxDrainCommands = 512;

    @Option(names = "--executorDrainMillis", defaultValue = "2", description = "Executor drain time budget in milliseconds.")
    public long executorDrainTimeLimitMillis = 2;

    @Option(
            names = "--transactionQueueMaxCommands",
            defaultValue = "" + DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS,
            description = "Transaction queue max commands for MULTI (0 disables)."
    )
    public int transactionQueueMaxCommands = DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS;

    @Option(
            names = "--transactionQueueMaxBytes",
            defaultValue = "" + DEFAULT_TRANSACTION_QUEUE_MAX_BYTES,
            description = "Transaction queue max bytes for MULTI (0 disables)."
    )
    public long transactionQueueMaxBytes = DEFAULT_TRANSACTION_QUEUE_MAX_BYTES;

    @Option(
            names = "--protocolMaxBulkBytes",
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_BULK_BYTES,
            description = "Protocol max request payload bytes."
    )
    public int protocolMaxBulkBytes = DEFAULT_PROTOCOL_MAX_BULK_BYTES;

    @Option(
            names = "--protocolMaxArgs",
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_ARGS,
            description = "Protocol max args per command."
    )
    public int protocolMaxArgs = DEFAULT_PROTOCOL_MAX_ARGS;

    @Option(
            names = "--protocolMaxLineBytes",
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_LINE_BYTES,
            description = "Protocol max header bytes."
    )
    public int protocolMaxLineBytes = DEFAULT_PROTOCOL_MAX_LINE_BYTES;

    @Option(
            names = "--protocolMaxCommandBytes",
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_COMMAND_BYTES,
            description = "Protocol max cumulative bytes per command."
    )
    public int protocolMaxCommandBytes = DEFAULT_PROTOCOL_MAX_COMMAND_BYTES;

    @Option(
            names = "--protocolGlobalInFlightBytes",
            defaultValue = "0",
            description = "Global RESP ingress in-flight memory limit (0 derives from executor queue bytes)."
    )
    public long protocolGlobalInFlightBytes;

    @Option(
            names = "--client-idle-timeout-millis",
            defaultValue = "0",
            description = "Close clients idle for this many milliseconds (0 disables)."
    )
    public long clientIdleTimeoutMillis = 0;

    @Option(
            names = "--client-output-buffer-limit-bytes",
            defaultValue = "67108864",
            description = "Close slow clients above this outbound buffer size (0 disables)."
    )
    public long clientOutputBufferLimitBytes = 67108864;

    @Option(
            names = "--client-output-buffer-over-limit-millis",
            defaultValue = "10000",
            description = "Slow-client grace period above output buffer limit in milliseconds."
    )
    public long clientOutputBufferOverLimitMillis = 10000;

    @Option(
            names = "--replyGlobalCapacityBytes",
            defaultValue = "" + DEFAULT_REPLY_GLOBAL_CAPACITY_BYTES,
            description = "Hard global RESP reply capacity in bytes."
    )
    public long replyGlobalCapacityBytes = DEFAULT_REPLY_GLOBAL_CAPACITY_BYTES;

    @Option(
            names = "--replyPerConnectionCapacityBytes",
            defaultValue = "" + DEFAULT_REPLY_PER_CONNECTION_CAPACITY_BYTES,
            description = "Hard per-connection RESP reply capacity in bytes."
    )
    public long replyPerConnectionCapacityBytes = DEFAULT_REPLY_PER_CONNECTION_CAPACITY_BYTES;

    @Option(
            names = "--replyMaxTotalBytes",
            defaultValue = "" + DEFAULT_REPLY_MAX_TOTAL_BYTES,
            description = "Hard total charge for one top-level RESP reply in bytes."
    )
    public long replyMaxTotalBytes = DEFAULT_REPLY_MAX_TOTAL_BYTES;

    @Option(
            names = "--replyChunkPayloadBytes",
            defaultValue = "" + DEFAULT_REPLY_CHUNK_PAYLOAD_BYTES,
            description = "Fixed RESP reply chunk payload capacity in bytes."
    )
    public int replyChunkPayloadBytes = DEFAULT_REPLY_CHUNK_PAYLOAD_BYTES;

    @Option(
            names = "--replyControlReservationBytes",
            defaultValue = "" + DEFAULT_REPLY_CONTROL_RESERVATION_BYTES,
            description = "Per-request RESP reply control reservation in bytes."
    )
    public long replyControlReservationBytes = DEFAULT_REPLY_CONTROL_RESERVATION_BYTES;

    @Option(
            names = "--replyDrainTimeoutMillis",
            defaultValue = "" + DEFAULT_REPLY_DRAIN_TIMEOUT_MILLIS,
            description = "Graceful RESP reply drain timeout in milliseconds."
    )
    public long replyDrainTimeoutMillis = DEFAULT_REPLY_DRAIN_TIMEOUT_MILLIS;

    @Option(names = "--maxmemoryBytes", defaultValue = "0", description = "Maxmemory in bytes (0 disables eviction).")
    public long maxmemoryBytes = 0;

    @Option(
            names = "--maxmemoryScope",
            defaultValue = "global",
            description = "Maxmemory scope: global|per-db."
    )
    public String maxmemoryScope = "global";

    @Option(names = "--maxmemoryPolicy", defaultValue = "noeviction", description = "Maxmemory policy string.")
    public String maxmemoryPolicy = "noeviction";

    @Option(names = "--maxmemorySamples", defaultValue = "5", description = "Maxmemory samples (policy dependent).")
    public int maxmemorySamples = 5;

    @Option(names = "--evictionTimeLimitMillis", defaultValue = "5", description = "Eviction time budget per tick in milliseconds.")
    public long evictionTimeLimitMillis = 5;

    @Option(names = "--expireCleanupTimeLimitMillis", defaultValue = "5", description = "Expire cleanup time budget per tick in milliseconds.")
    public long expireCleanupTimeLimitMillis = 5;

    @Option(names = "--nativeDefragEnabled", description = "Enable DB native allocator defrag during maintenance ticks.")
    public boolean nativeDefragEnabled;

    @Option(
            names = "--nativeDefragMaxMoveBytes",
            defaultValue = "65536",
            description = "Native defrag max bytes to move per maintenance tick."
    )
    public long nativeDefragMaxMoveBytes = 64L * 1024L;

    @Option(
            names = "--nativeDefragMaxObjects",
            defaultValue = "64",
            description = "Native defrag max objects to inspect per maintenance tick."
    )
    public long nativeDefragMaxObjects = 64L;

    @Option(
            names = "--nativeDefragTimeLimitMillis",
            defaultValue = "1",
            description = "Native defrag time budget per maintenance tick in milliseconds."
    )
    public long nativeDefragTimeLimitMillis = 1L;

    @Option(
            names = "--nativeSlotCapacity",
            defaultValue = "0",
            description = "Override DB shared native object slot capacity (0 keeps default)."
    )
    public int nativeSlotCapacity;

    @Option(
            names = "--keysTimeBudgetMillis",
            defaultValue = "0",
            description = "KEYS time budget in milliseconds (0 disables; use SCAN for large datasets)."
    )
    public long keysTimeBudgetMillis = 0;

    @Option(
            names = "--keysMaxResults",
            defaultValue = "" + Integer.MAX_VALUE,
            description = "KEYS max results (0 disables KEYS; default unlimited)."
    )
    public int keysMaxResults = Integer.MAX_VALUE;

    public void normalizeAndValidate() {
        if (noCleanup) {
            cleanupIntervalMillis = 0;
        }
        if (bind != null) {
            bind = bind.trim();
        }
        executorSchedulingPolicy = normalizeExecutorSchedulingPolicy(executorSchedulingPolicy);
        maxmemoryScope = normalizeMaxmemoryScope(maxmemoryScope);
        maxmemoryPolicy = normalizeMaxmemoryPolicy(maxmemoryPolicy);
        toRuntimeConfig().executorConfig();
    }

    /**
     * Convert normalized CLI args into the canonical runtime config.
     * <p>
     * Callers should invoke {@link #normalizeAndValidate()} first so the config reflects stable argv values.
     */
    public YierdisServerRuntimeConfig toRuntimeConfig() {
        return new YierdisServerRuntimeConfig(
                bind,
                port,
                maxClients,
                databases,
                cleanupIntervalMillis,
                ioThreads,
                executorQueueCapacity,
                executorQueueMaxBytes,
                parseExecutorSchedulingPolicy(executorSchedulingPolicy),
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
                YierdisServerRuntimeConfig.MaxmemoryScope.fromArgvValue(maxmemoryScope),
                MaxmemoryPolicy.parse(maxmemoryPolicy),
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

    private static String normalizeExecutorSchedulingPolicy(String rawValue) {
        return parseExecutorSchedulingPolicy(rawValue).name().toLowerCase(java.util.Locale.ROOT);
    }

    private static yier.bubu.redis.execution.executor.SchedulingPolicy parseExecutorSchedulingPolicy(
            String rawValue
    ) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("executorSchedulingPolicy must not be blank");
        }
        try {
            return yier.bubu.redis.execution.executor.SchedulingPolicy.valueOf(
                    rawValue.trim().toUpperCase(java.util.Locale.ROOT)
            );
        } catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException("unsupported executorSchedulingPolicy: " + rawValue);
        }
    }

    private static String normalizeMaxmemoryScope(String rawValue) {
        return YierdisServerRuntimeConfig.MaxmemoryScope.parseCliValue(rawValue).argvValue();
    }

    private static String normalizeMaxmemoryPolicy(String rawValue) {
        return MaxmemoryPolicy.parse(rawValue).redisName();
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
