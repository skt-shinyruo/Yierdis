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

    private static final long DEFAULT_EXECUTOR_QUEUE_MAX_BYTES = 64L * 1024 * 1024; // 64 MiB
    private static final int DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS = 1024;
    private static final long DEFAULT_TRANSACTION_QUEUE_MAX_BYTES = DEFAULT_EXECUTOR_QUEUE_MAX_BYTES;
    private static final long DEFAULT_BACKPRESSURE_BYTES_HIGH = 16L * 1024 * 1024; // 16 MiB
    private static final long DEFAULT_BACKPRESSURE_BYTES_LOW = 8L * 1024 * 1024; // 8 MiB

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    public boolean help;

    @Option(names = YierdisServerArgNames.PORT, defaultValue = "6378", description = "TCP port to bind.")
    public int port = 6378;

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
            names = YierdisServerArgNames.CLIENT_IDLE_TIMEOUT_MILLIS,
            defaultValue = "300000",
            description = "Close clients idle for this many milliseconds (0 disables)."
    )
    public long clientIdleTimeoutMillis = 300000;

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
            names = YierdisServerArgNames.KEYS_TIME_BUDGET_MILLIS,
            defaultValue = "20",
            description = "KEYS time budget in milliseconds (0 disables; use SCAN for large datasets)."
    )
    public long keysTimeBudgetMillis = 20;

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

        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be in range 0..65535");
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
        if (protocolMaxArgs <= 0) {
            throw new IllegalArgumentException("protocolMaxArgs must be > 0");
        }
        if (protocolMaxLineBytes <= 0) {
            throw new IllegalArgumentException("protocolMaxLineBytes must be > 0");
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
        out.port = port;
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
        out.clientIdleTimeoutMillis = clientIdleTimeoutMillis;
        out.clientOutputBufferLimitBytes = clientOutputBufferLimitBytes;
        out.clientOutputBufferOverLimitMillis = clientOutputBufferOverLimitMillis;
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
                port,
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
                clientIdleTimeoutMillis,
                clientOutputBufferLimitBytes,
                clientOutputBufferOverLimitMillis,
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
                keysTimeBudgetMillis,
                keysMaxResults
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

        out.add(YierdisServerArgNames.PORT);
        out.add(Integer.toString(port));

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

        out.add(YierdisServerArgNames.CLIENT_IDLE_TIMEOUT_MILLIS);
        out.add(Long.toString(clientIdleTimeoutMillis));
        out.add(YierdisServerArgNames.CLIENT_OUTPUT_BUFFER_LIMIT_BYTES);
        out.add(Long.toString(clientOutputBufferLimitBytes));
        out.add(YierdisServerArgNames.CLIENT_OUTPUT_BUFFER_OVER_LIMIT_MILLIS);
        out.add(Long.toString(clientOutputBufferOverLimitMillis));

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
}
