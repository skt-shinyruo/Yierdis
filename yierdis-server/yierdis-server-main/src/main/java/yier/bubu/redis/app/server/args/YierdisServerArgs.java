package yier.bubu.redis.app.server.args;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.ArrayList;
import java.util.List;

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

    @Option(names = YierdisServerArgNames.BIND, defaultValue = "127.0.0.1", description = "TCP host or address to bind.")
    public String bind = "127.0.0.1";

    @Option(names = YierdisServerArgNames.PORT, defaultValue = "6378", description = "TCP port to bind.")
    public int port = 6378;

    @Option(names = YierdisServerArgNames.MAX_CLIENTS, defaultValue = "1024", description = "Maximum accepted client connections.")
    public int maxClients = 1024;

    @Option(
            names = YierdisServerArgNames.DATABASES,
            defaultValue = "16",
            description = "Number of logical databases (SELECT 0..N-1)."
    )
    public int databases = 16;

    @Option(
            names = YierdisServerArgNames.CLEANUP_INTERVAL_MILLIS,
            defaultValue = "1000",
            description = "Expiration cleanup interval in milliseconds (0 disables cleanup)."
    )
    public long cleanupIntervalMillis = 1000;

    @Option(names = YierdisServerArgNames.NO_CLEANUP, description = "Disable periodic expiration cleanup.")
    public boolean noCleanup;

    @Option(names = YierdisServerArgNames.IO_THREADS, defaultValue = "1", description = "Netty I/O threads.")
    public int ioThreads = 1;

    @Option(
            names = YierdisServerArgNames.EXECUTOR_QUEUE_CAPACITY,
            defaultValue = "1024",
            description = "Command executor queue capacity."
    )
    public int executorQueueCapacity = 1024;

    @Option(
            names = YierdisServerArgNames.EXECUTOR_QUEUE_MAX_BYTES,
            defaultValue = "" + DEFAULT_EXECUTOR_QUEUE_MAX_BYTES,
            description = "Command executor queue max bytes (0 disables)."
    )
    public long executorQueueMaxBytes = DEFAULT_EXECUTOR_QUEUE_MAX_BYTES;

    @Option(
            names = YierdisServerArgNames.EXECUTOR_SCHEDULING_POLICY,
            defaultValue = "fair",
            description = "Executor scheduling policy: global|fair."
    )
    public String executorSchedulingPolicy = "fair";

    @Option(names = YierdisServerArgNames.BACKPRESSURE_HIGH, defaultValue = "256", description = "Backpressure high watermark.")
    public int backpressureHighWatermark = 256;

    @Option(names = YierdisServerArgNames.BACKPRESSURE_LOW, defaultValue = "128", description = "Backpressure low watermark.")
    public int backpressureLowWatermark = 128;

    @Option(
            names = YierdisServerArgNames.BACKPRESSURE_BYTES_HIGH,
            defaultValue = "" + DEFAULT_BACKPRESSURE_BYTES_HIGH,
            description = "Backpressure bytes high watermark (0 disables)."
    )
    public long backpressureBytesHighWatermark = DEFAULT_BACKPRESSURE_BYTES_HIGH;

    @Option(
            names = YierdisServerArgNames.BACKPRESSURE_BYTES_LOW,
            defaultValue = "" + DEFAULT_BACKPRESSURE_BYTES_LOW,
            description = "Backpressure bytes low watermark (0 disables)."
    )
    public long backpressureBytesLowWatermark = DEFAULT_BACKPRESSURE_BYTES_LOW;

    @Option(names = YierdisServerArgNames.EXECUTOR_MAX_DRAIN, defaultValue = "512", description = "Max commands drained per executor tick.")
    public int executorMaxDrainCommands = 512;

    @Option(names = YierdisServerArgNames.EXECUTOR_DRAIN_MILLIS, defaultValue = "2", description = "Executor drain time budget in milliseconds.")
    public long executorDrainTimeLimitMillis = 2;

    @Option(
            names = YierdisServerArgNames.TRANSACTION_QUEUE_MAX_COMMANDS,
            defaultValue = "" + DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS,
            description = "Transaction queue max commands for MULTI (0 disables)."
    )
    public int transactionQueueMaxCommands = DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS;

    @Option(
            names = YierdisServerArgNames.TRANSACTION_QUEUE_MAX_BYTES,
            defaultValue = "" + DEFAULT_TRANSACTION_QUEUE_MAX_BYTES,
            description = "Transaction queue max bytes for MULTI (0 disables)."
    )
    public long transactionQueueMaxBytes = DEFAULT_TRANSACTION_QUEUE_MAX_BYTES;

    @Option(
            names = YierdisServerArgNames.PROTOCOL_MAX_BULK_BYTES,
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_BULK_BYTES,
            description = "Protocol max request payload bytes."
    )
    public int protocolMaxBulkBytes = DEFAULT_PROTOCOL_MAX_BULK_BYTES;

    @Option(
            names = YierdisServerArgNames.PROTOCOL_MAX_ARGS,
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_ARGS,
            description = "Protocol max args per command."
    )
    public int protocolMaxArgs = DEFAULT_PROTOCOL_MAX_ARGS;

    @Option(
            names = YierdisServerArgNames.PROTOCOL_MAX_LINE_BYTES,
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_LINE_BYTES,
            description = "Protocol max header bytes."
    )
    public int protocolMaxLineBytes = DEFAULT_PROTOCOL_MAX_LINE_BYTES;

    @Option(
            names = YierdisServerArgNames.PROTOCOL_MAX_COMMAND_BYTES,
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_COMMAND_BYTES,
            description = "Protocol max cumulative bytes per command."
    )
    public int protocolMaxCommandBytes = DEFAULT_PROTOCOL_MAX_COMMAND_BYTES;

    @Option(
            names = YierdisServerArgNames.PROTOCOL_GLOBAL_IN_FLIGHT_BYTES,
            defaultValue = "0",
            description = "Global RESP ingress in-flight memory limit (0 derives from executor queue bytes)."
    )
    public long protocolGlobalInFlightBytes;

    @Option(
            names = YierdisServerArgNames.CLIENT_IDLE_TIMEOUT_MILLIS,
            defaultValue = "0",
            description = "Close clients idle for this many milliseconds (0 disables)."
    )
    public long clientIdleTimeoutMillis = 0;

    @Option(
            names = YierdisServerArgNames.CLIENT_OUTPUT_BUFFER_LIMIT_BYTES,
            defaultValue = "67108864",
            description = "Close slow clients above this outbound buffer size (0 disables)."
    )
    public long clientOutputBufferLimitBytes = 67108864;

    @Option(
            names = YierdisServerArgNames.CLIENT_OUTPUT_BUFFER_OVER_LIMIT_MILLIS,
            defaultValue = "10000",
            description = "Slow-client grace period above output buffer limit in milliseconds."
    )
    public long clientOutputBufferOverLimitMillis = 10000;

    @Option(
            names = YierdisServerArgNames.REPLY_GLOBAL_CAPACITY_BYTES,
            defaultValue = "" + DEFAULT_REPLY_GLOBAL_CAPACITY_BYTES,
            description = "Hard global RESP reply capacity in bytes."
    )
    public long replyGlobalCapacityBytes = DEFAULT_REPLY_GLOBAL_CAPACITY_BYTES;

    @Option(
            names = YierdisServerArgNames.REPLY_PER_CONNECTION_CAPACITY_BYTES,
            defaultValue = "" + DEFAULT_REPLY_PER_CONNECTION_CAPACITY_BYTES,
            description = "Hard per-connection RESP reply capacity in bytes."
    )
    public long replyPerConnectionCapacityBytes = DEFAULT_REPLY_PER_CONNECTION_CAPACITY_BYTES;

    @Option(
            names = YierdisServerArgNames.REPLY_MAX_TOTAL_BYTES,
            defaultValue = "" + DEFAULT_REPLY_MAX_TOTAL_BYTES,
            description = "Hard total charge for one top-level RESP reply in bytes."
    )
    public long replyMaxTotalBytes = DEFAULT_REPLY_MAX_TOTAL_BYTES;

    @Option(
            names = YierdisServerArgNames.REPLY_CHUNK_PAYLOAD_BYTES,
            defaultValue = "" + DEFAULT_REPLY_CHUNK_PAYLOAD_BYTES,
            description = "Fixed RESP reply chunk payload capacity in bytes."
    )
    public int replyChunkPayloadBytes = DEFAULT_REPLY_CHUNK_PAYLOAD_BYTES;

    @Option(
            names = YierdisServerArgNames.REPLY_CONTROL_RESERVATION_BYTES,
            defaultValue = "" + DEFAULT_REPLY_CONTROL_RESERVATION_BYTES,
            description = "Per-request RESP reply control reservation in bytes."
    )
    public long replyControlReservationBytes = DEFAULT_REPLY_CONTROL_RESERVATION_BYTES;

    @Option(
            names = YierdisServerArgNames.REPLY_DRAIN_TIMEOUT_MILLIS,
            defaultValue = "" + DEFAULT_REPLY_DRAIN_TIMEOUT_MILLIS,
            description = "Graceful RESP reply drain timeout in milliseconds."
    )
    public long replyDrainTimeoutMillis = DEFAULT_REPLY_DRAIN_TIMEOUT_MILLIS;

    @Option(names = YierdisServerArgNames.MAXMEMORY_BYTES, defaultValue = "0", description = "Maxmemory in bytes (0 disables eviction).")
    public long maxmemoryBytes = 0;

    @Option(
            names = YierdisServerArgNames.MAXMEMORY_SCOPE,
            defaultValue = "global",
            description = "Maxmemory scope: global|per-db."
    )
    public String maxmemoryScope = "global";

    @Option(names = YierdisServerArgNames.MAXMEMORY_POLICY, defaultValue = "noeviction", description = "Maxmemory policy string.")
    public String maxmemoryPolicy = "noeviction";

    @Option(names = YierdisServerArgNames.MAXMEMORY_SAMPLES, defaultValue = "5", description = "Maxmemory samples (policy dependent).")
    public int maxmemorySamples = 5;

    @Option(names = YierdisServerArgNames.EVICTION_TIME_LIMIT_MILLIS, defaultValue = "5", description = "Eviction time budget per tick in milliseconds.")
    public long evictionTimeLimitMillis = 5;

    @Option(names = YierdisServerArgNames.EXPIRE_CLEANUP_TIME_LIMIT_MILLIS, defaultValue = "5", description = "Expire cleanup time budget per tick in milliseconds.")
    public long expireCleanupTimeLimitMillis = 5;

    @Option(names = YierdisServerArgNames.NATIVE_DEFRAG_ENABLED, description = "Enable DB native allocator defrag during maintenance ticks.")
    public boolean nativeDefragEnabled;

    @Option(
            names = YierdisServerArgNames.NATIVE_DEFRAG_MAX_MOVE_BYTES,
            defaultValue = "65536",
            description = "Native defrag max bytes to move per maintenance tick."
    )
    public long nativeDefragMaxMoveBytes = 64L * 1024L;

    @Option(
            names = YierdisServerArgNames.NATIVE_DEFRAG_MAX_OBJECTS,
            defaultValue = "64",
            description = "Native defrag max objects to inspect per maintenance tick."
    )
    public long nativeDefragMaxObjects = 64L;

    @Option(
            names = YierdisServerArgNames.NATIVE_DEFRAG_TIME_LIMIT_MILLIS,
            defaultValue = "1",
            description = "Native defrag time budget per maintenance tick in milliseconds."
    )
    public long nativeDefragTimeLimitMillis = 1L;

    @Option(
            names = YierdisServerArgNames.NATIVE_SLOT_CAPACITY,
            defaultValue = "0",
            description = "Override DB shared native object slot capacity (0 keeps default)."
    )
    public int nativeSlotCapacity;

    @Option(
            names = YierdisServerArgNames.KEYS_TIME_BUDGET_MILLIS,
            defaultValue = "0",
            description = "KEYS time budget in milliseconds (0 disables; use SCAN for large datasets)."
    )
    public long keysTimeBudgetMillis = 0;

    @Option(
            names = YierdisServerArgNames.KEYS_MAX_RESULTS,
            defaultValue = "" + Integer.MAX_VALUE,
            description = "KEYS max results (0 disables KEYS; default unlimited)."
    )
    public int keysMaxResults = Integer.MAX_VALUE;

    public void normalizeAndValidate() {
        if (noCleanup) {
            cleanupIntervalMillis = 0;
        }

        if (bind == null || bind.isBlank()) {
            throw new IllegalArgumentException("bind must not be blank");
        }
        bind = bind.trim();
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be in range 0..65535");
        }
        if (maxClients <= 0) {
            throw new IllegalArgumentException("maxClients must be > 0");
        }
        if (databases <= 0) {
            throw new IllegalArgumentException("databases must be > 0");
        }
        if (databases > 1024) {
            throw new IllegalArgumentException("databases must be <= 1024");
        }
        if (cleanupIntervalMillis < 0) {
            throw new IllegalArgumentException("cleanupIntervalMillis must be >= 0");
        }
        if (ioThreads <= 0) {
            throw new IllegalArgumentException("ioThreads must be > 0");
        }
        if (executorQueueCapacity <= 0) {
            throw new IllegalArgumentException("executorQueueCapacity must be > 0");
        }
        if (executorQueueMaxBytes < 0) {
            throw new IllegalArgumentException("executorQueueMaxBytes must be >= 0");
        }
        executorSchedulingPolicy = normalizeExecutorSchedulingPolicy(executorSchedulingPolicy);
        if (backpressureHighWatermark <= 0) {
            throw new IllegalArgumentException("backpressureHighWatermark must be > 0");
        }
        if (backpressureLowWatermark < 0) {
            throw new IllegalArgumentException("backpressureLowWatermark must be >= 0");
        }
        if (backpressureLowWatermark >= backpressureHighWatermark) {
            throw new IllegalArgumentException("backpressureLowWatermark must be < backpressureHighWatermark");
        }
        if (backpressureBytesHighWatermark < 0) {
            throw new IllegalArgumentException("backpressureBytesHighWatermark must be >= 0");
        }
        if (backpressureBytesLowWatermark < 0) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be >= 0");
        }
        if (backpressureBytesHighWatermark == 0 && backpressureBytesLowWatermark != 0) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be 0 when backpressureBytesHighWatermark is 0");
        }
        if (backpressureBytesHighWatermark > 0 && backpressureBytesLowWatermark >= backpressureBytesHighWatermark) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be < backpressureBytesHighWatermark");
        }
        if (executorMaxDrainCommands <= 0) {
            throw new IllegalArgumentException("executorMaxDrainCommands must be > 0");
        }
        if (executorDrainTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("executorDrainTimeLimitMillis must be > 0");
        }
        if (transactionQueueMaxCommands < 0) {
            throw new IllegalArgumentException("transactionQueueMaxCommands must be >= 0");
        }
        if (transactionQueueMaxBytes < 0) {
            throw new IllegalArgumentException("transactionQueueMaxBytes must be >= 0");
        }
        if (protocolMaxBulkBytes <= 0) {
            throw new IllegalArgumentException("protocolMaxBulkBytes must be > 0");
        }
        if (protocolMaxBulkBytes > RespProtocolLimits.MAX_BULK_BYTES) {
            throw new IllegalArgumentException("protocolMaxBulkBytes must be <= " + RespProtocolLimits.MAX_BULK_BYTES);
        }
        if (protocolMaxArgs <= 0) {
            throw new IllegalArgumentException("protocolMaxArgs must be > 0");
        }
        if (protocolMaxArgs > RespProtocolLimits.MAX_ARGS) {
            throw new IllegalArgumentException("protocolMaxArgs must be <= " + RespProtocolLimits.MAX_ARGS);
        }
        if (protocolMaxLineBytes <= 0) {
            throw new IllegalArgumentException("protocolMaxLineBytes must be > 0");
        }
        if (protocolMaxCommandBytes <= 0) {
            throw new IllegalArgumentException("protocolMaxCommandBytes must be > 0");
        }
        if (protocolMaxCommandBytes > RespProtocolLimits.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException("protocolMaxCommandBytes must be <= " + RespProtocolLimits.MAX_COMMAND_BYTES);
        }
        if (protocolGlobalInFlightBytes < 0) {
            throw new IllegalArgumentException("protocolGlobalInFlightBytes must be >= 0");
        }
        if (clientIdleTimeoutMillis < 0) {
            throw new IllegalArgumentException("clientIdleTimeoutMillis must be >= 0");
        }
        if (clientOutputBufferLimitBytes < 0) {
            throw new IllegalArgumentException("clientOutputBufferLimitBytes must be >= 0");
        }
        if (clientOutputBufferOverLimitMillis < 0) {
            throw new IllegalArgumentException("clientOutputBufferOverLimitMillis must be >= 0");
        }
        if (clientOutputBufferLimitBytes > 0 && clientOutputBufferOverLimitMillis <= 0) {
            throw new IllegalArgumentException("clientOutputBufferOverLimitMillis must be > 0 when clientOutputBufferLimitBytes is enabled");
        }
        if (replyGlobalCapacityBytes <= 0L) {
            throw new IllegalArgumentException("replyGlobalCapacityBytes must be > 0");
        }
        if (replyPerConnectionCapacityBytes <= 0L) {
            throw new IllegalArgumentException("replyPerConnectionCapacityBytes must be > 0");
        }
        if (replyMaxTotalBytes <= 0L) {
            throw new IllegalArgumentException("replyMaxTotalBytes must be > 0");
        }
        if (replyChunkPayloadBytes <= 0) {
            throw new IllegalArgumentException("replyChunkPayloadBytes must be > 0");
        }
        if (replyControlReservationBytes <= 0L) {
            throw new IllegalArgumentException("replyControlReservationBytes must be > 0");
        }
        if (replyControlReservationBytes < YierdisServerRuntimeConfig.MIN_REPLY_CONTROL_RESERVATION_BYTES) {
            throw new IllegalArgumentException(
                    "replyControlReservationBytes must fit reply fixed overhead and the largest scalar error frame"
            );
        }
        if (replyDrainTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("replyDrainTimeoutMillis must be > 0");
        }
        if (replyControlReservationBytes > replyMaxTotalBytes) {
            throw new IllegalArgumentException("replyControlReservationBytes must be <= replyMaxTotalBytes");
        }
        if (replyMaxTotalBytes > replyPerConnectionCapacityBytes) {
            throw new IllegalArgumentException("replyMaxTotalBytes must be <= replyPerConnectionCapacityBytes");
        }
        if (replyPerConnectionCapacityBytes > replyGlobalCapacityBytes) {
            throw new IllegalArgumentException("replyPerConnectionCapacityBytes must be <= replyGlobalCapacityBytes");
        }
        long minimumReplyCharge = saturatedAdd(
                saturatedAdd(replyControlReservationBytes, replyChunkPayloadBytes),
                YierdisServerRuntimeConfig.REPLY_FIXED_OVERHEAD_BYTES
        );
        if (minimumReplyCharge > replyMaxTotalBytes) {
            throw new IllegalArgumentException("reply chunk, control, and fixed overhead must fit replyMaxTotalBytes");
        }
        if (maxmemoryBytes < 0) {
            throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
        }
        maxmemoryScope = normalizeMaxmemoryScope(maxmemoryScope);
        maxmemoryPolicy = normalizeMaxmemoryPolicy(maxmemoryPolicy);
        if (maxmemorySamples <= 0) {
            throw new IllegalArgumentException("maxmemorySamples must be > 0");
        }
        if (evictionTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("evictionTimeLimitMillis must be > 0");
        }
        if (expireCleanupTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("expireCleanupTimeLimitMillis must be > 0");
        }
        if (nativeDefragMaxMoveBytes < 0) {
            throw new IllegalArgumentException("nativeDefragMaxMoveBytes must be >= 0");
        }
        if (nativeDefragMaxObjects < 0) {
            throw new IllegalArgumentException("nativeDefragMaxObjects must be >= 0");
        }
        if (nativeDefragTimeLimitMillis < 0) {
            throw new IllegalArgumentException("nativeDefragTimeLimitMillis must be >= 0");
        }
        if (nativeSlotCapacity < 0) {
            throw new IllegalArgumentException("nativeSlotCapacity must be >= 0");
        }
        if (keysTimeBudgetMillis < 0) {
            throw new IllegalArgumentException("keysTimeBudgetMillis must be >= 0");
        }
        if (keysMaxResults < 0) {
            throw new IllegalArgumentException("keysMaxResults must be >= 0");
        }
    }

    public YierdisServerArgs copy() {
        YierdisServerArgs out = new YierdisServerArgs();
        out.help = help;
        out.bind = bind;
        out.port = port;
        out.maxClients = maxClients;
        out.databases = databases;
        out.cleanupIntervalMillis = cleanupIntervalMillis;
        out.noCleanup = noCleanup;
        out.ioThreads = ioThreads;
        out.executorQueueCapacity = executorQueueCapacity;
        out.executorQueueMaxBytes = executorQueueMaxBytes;
        out.executorSchedulingPolicy = executorSchedulingPolicy;
        out.backpressureHighWatermark = backpressureHighWatermark;
        out.backpressureLowWatermark = backpressureLowWatermark;
        out.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        out.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        out.executorMaxDrainCommands = executorMaxDrainCommands;
        out.executorDrainTimeLimitMillis = executorDrainTimeLimitMillis;
        out.transactionQueueMaxCommands = transactionQueueMaxCommands;
        out.transactionQueueMaxBytes = transactionQueueMaxBytes;
        out.protocolMaxBulkBytes = protocolMaxBulkBytes;
        out.protocolMaxArgs = protocolMaxArgs;
        out.protocolMaxLineBytes = protocolMaxLineBytes;
        out.protocolMaxCommandBytes = protocolMaxCommandBytes;
        out.protocolGlobalInFlightBytes = protocolGlobalInFlightBytes;
        out.clientIdleTimeoutMillis = clientIdleTimeoutMillis;
        out.clientOutputBufferLimitBytes = clientOutputBufferLimitBytes;
        out.clientOutputBufferOverLimitMillis = clientOutputBufferOverLimitMillis;
        out.replyGlobalCapacityBytes = replyGlobalCapacityBytes;
        out.replyPerConnectionCapacityBytes = replyPerConnectionCapacityBytes;
        out.replyMaxTotalBytes = replyMaxTotalBytes;
        out.replyChunkPayloadBytes = replyChunkPayloadBytes;
        out.replyControlReservationBytes = replyControlReservationBytes;
        out.replyDrainTimeoutMillis = replyDrainTimeoutMillis;
        out.maxmemoryBytes = maxmemoryBytes;
        out.maxmemoryScope = maxmemoryScope;
        out.maxmemoryPolicy = maxmemoryPolicy;
        out.maxmemorySamples = maxmemorySamples;
        out.evictionTimeLimitMillis = evictionTimeLimitMillis;
        out.expireCleanupTimeLimitMillis = expireCleanupTimeLimitMillis;
        out.nativeDefragEnabled = nativeDefragEnabled;
        out.nativeDefragMaxMoveBytes = nativeDefragMaxMoveBytes;
        out.nativeDefragMaxObjects = nativeDefragMaxObjects;
        out.nativeDefragTimeLimitMillis = nativeDefragTimeLimitMillis;
        out.nativeSlotCapacity = nativeSlotCapacity;
        out.keysTimeBudgetMillis = keysTimeBudgetMillis;
        out.keysMaxResults = keysMaxResults;
        return out;
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
                YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.fromArgvValue(executorSchedulingPolicy),
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

    /**
     * 将当前参数对象转换为可执行的命令行 argv（flag + value），用于工具/脚本复用同一份 SSOT。
     * <p>
     * 说明：该方法不会隐式调用 {@link #normalizeAndValidate()}；调用方应先完成校验以保证输出稳定。
     */
    public List<String> toArgv() {
        List<String> out = new ArrayList<>();
        if (help) {
            out.add("--help");
            return out;
        }

        out.add(YierdisServerArgNames.BIND);
        out.add(bind);
        out.add(YierdisServerArgNames.PORT);
        out.add(Integer.toString(port));
        out.add(YierdisServerArgNames.MAX_CLIENTS);
        out.add(Integer.toString(maxClients));

        out.add(YierdisServerArgNames.DATABASES);
        out.add(Integer.toString(databases));

        if (noCleanup) {
            out.add(YierdisServerArgNames.NO_CLEANUP);
        } else {
            out.add(YierdisServerArgNames.CLEANUP_INTERVAL_MILLIS);
            out.add(Long.toString(cleanupIntervalMillis));
        }

        out.add(YierdisServerArgNames.IO_THREADS);
        out.add(Integer.toString(ioThreads));

        out.add(YierdisServerArgNames.EXECUTOR_QUEUE_CAPACITY);
        out.add(Integer.toString(executorQueueCapacity));
        out.add(YierdisServerArgNames.EXECUTOR_QUEUE_MAX_BYTES);
        out.add(Long.toString(executorQueueMaxBytes));
        out.add(YierdisServerArgNames.EXECUTOR_SCHEDULING_POLICY);
        out.add(executorSchedulingPolicy);
        out.add(YierdisServerArgNames.BACKPRESSURE_HIGH);
        out.add(Integer.toString(backpressureHighWatermark));
        out.add(YierdisServerArgNames.BACKPRESSURE_LOW);
        out.add(Integer.toString(backpressureLowWatermark));
        out.add(YierdisServerArgNames.BACKPRESSURE_BYTES_HIGH);
        out.add(Long.toString(backpressureBytesHighWatermark));
        out.add(YierdisServerArgNames.BACKPRESSURE_BYTES_LOW);
        out.add(Long.toString(backpressureBytesLowWatermark));
        out.add(YierdisServerArgNames.EXECUTOR_MAX_DRAIN);
        out.add(Integer.toString(executorMaxDrainCommands));
        out.add(YierdisServerArgNames.EXECUTOR_DRAIN_MILLIS);
        out.add(Long.toString(executorDrainTimeLimitMillis));

        out.add(YierdisServerArgNames.TRANSACTION_QUEUE_MAX_COMMANDS);
        out.add(Integer.toString(transactionQueueMaxCommands));
        out.add(YierdisServerArgNames.TRANSACTION_QUEUE_MAX_BYTES);
        out.add(Long.toString(transactionQueueMaxBytes));

        out.add(YierdisServerArgNames.PROTOCOL_MAX_BULK_BYTES);
        out.add(Integer.toString(protocolMaxBulkBytes));
        out.add(YierdisServerArgNames.PROTOCOL_MAX_ARGS);
        out.add(Integer.toString(protocolMaxArgs));
        out.add(YierdisServerArgNames.PROTOCOL_MAX_LINE_BYTES);
        out.add(Integer.toString(protocolMaxLineBytes));
        out.add(YierdisServerArgNames.PROTOCOL_MAX_COMMAND_BYTES);
        out.add(Integer.toString(protocolMaxCommandBytes));
        out.add(YierdisServerArgNames.PROTOCOL_GLOBAL_IN_FLIGHT_BYTES);
        out.add(Long.toString(protocolGlobalInFlightBytes));

        out.add(YierdisServerArgNames.CLIENT_IDLE_TIMEOUT_MILLIS);
        out.add(Long.toString(clientIdleTimeoutMillis));
        out.add(YierdisServerArgNames.CLIENT_OUTPUT_BUFFER_LIMIT_BYTES);
        out.add(Long.toString(clientOutputBufferLimitBytes));
        out.add(YierdisServerArgNames.CLIENT_OUTPUT_BUFFER_OVER_LIMIT_MILLIS);
        out.add(Long.toString(clientOutputBufferOverLimitMillis));

        out.add(YierdisServerArgNames.REPLY_GLOBAL_CAPACITY_BYTES);
        out.add(Long.toString(replyGlobalCapacityBytes));
        out.add(YierdisServerArgNames.REPLY_PER_CONNECTION_CAPACITY_BYTES);
        out.add(Long.toString(replyPerConnectionCapacityBytes));
        out.add(YierdisServerArgNames.REPLY_MAX_TOTAL_BYTES);
        out.add(Long.toString(replyMaxTotalBytes));
        out.add(YierdisServerArgNames.REPLY_CHUNK_PAYLOAD_BYTES);
        out.add(Integer.toString(replyChunkPayloadBytes));
        out.add(YierdisServerArgNames.REPLY_CONTROL_RESERVATION_BYTES);
        out.add(Long.toString(replyControlReservationBytes));
        out.add(YierdisServerArgNames.REPLY_DRAIN_TIMEOUT_MILLIS);
        out.add(Long.toString(replyDrainTimeoutMillis));

        out.add(YierdisServerArgNames.MAXMEMORY_BYTES);
        out.add(Long.toString(maxmemoryBytes));
        out.add(YierdisServerArgNames.MAXMEMORY_SCOPE);
        out.add(maxmemoryScope);
        out.add(YierdisServerArgNames.MAXMEMORY_POLICY);
        out.add(maxmemoryPolicy);
        out.add(YierdisServerArgNames.MAXMEMORY_SAMPLES);
        out.add(Integer.toString(maxmemorySamples));

        out.add(YierdisServerArgNames.EVICTION_TIME_LIMIT_MILLIS);
        out.add(Long.toString(evictionTimeLimitMillis));
        out.add(YierdisServerArgNames.EXPIRE_CLEANUP_TIME_LIMIT_MILLIS);
        out.add(Long.toString(expireCleanupTimeLimitMillis));

        if (nativeDefragEnabled) {
            out.add(YierdisServerArgNames.NATIVE_DEFRAG_ENABLED);
        }
        out.add(YierdisServerArgNames.NATIVE_DEFRAG_MAX_MOVE_BYTES);
        out.add(Long.toString(nativeDefragMaxMoveBytes));
        out.add(YierdisServerArgNames.NATIVE_DEFRAG_MAX_OBJECTS);
        out.add(Long.toString(nativeDefragMaxObjects));
        out.add(YierdisServerArgNames.NATIVE_DEFRAG_TIME_LIMIT_MILLIS);
        out.add(Long.toString(nativeDefragTimeLimitMillis));
        out.add(YierdisServerArgNames.NATIVE_SLOT_CAPACITY);
        out.add(Integer.toString(nativeSlotCapacity));

        out.add(YierdisServerArgNames.KEYS_TIME_BUDGET_MILLIS);
        out.add(Long.toString(keysTimeBudgetMillis));
        out.add(YierdisServerArgNames.KEYS_MAX_RESULTS);
        out.add(Integer.toString(keysMaxResults));

        return out;
    }

    private static String normalizeExecutorSchedulingPolicy(String rawValue) {
        return YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.parseCliValue(rawValue).argvValue();
    }

    private static String normalizeMaxmemoryScope(String rawValue) {
        return YierdisServerRuntimeConfig.MaxmemoryScope.parseCliValue(rawValue).argvValue();
    }

    private static String normalizeMaxmemoryPolicy(String rawValue) {
        return MaxmemoryPolicy.parse(rawValue).redisName();
    }

    private static long deriveProtocolGlobalInFlightBytes(long executorQueueMaxBytes, long configuredBytes) {
        if (configuredBytes > 0L) {
            return configuredBytes;
        }
        long queueBytes = Math.max(0L, executorQueueMaxBytes);
        long doubled = queueBytes > Long.MAX_VALUE / 2L ? Long.MAX_VALUE : queueBytes * 2L;
        return Math.max(MIN_PROTOCOL_GLOBAL_IN_FLIGHT_BYTES, doubled);
    }

    private static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
