package yier.bubu.redis.app.bench;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Command(
        name = "yierdis",
        description = "Server launch arguments used by YierdisBench.",
        sortOptions = false,
        usageHelpAutoWidth = true
)
public final class YierdisBenchServerArgs {
    private static final int DEFAULT_PROTOCOL_MAX_BULK_BYTES = RespProtocolLimits.DEFAULT_MAX_BULK_BYTES;
    private static final int DEFAULT_PROTOCOL_MAX_ARGS = RespProtocolLimits.DEFAULT_MAX_ARGS;
    private static final int DEFAULT_PROTOCOL_MAX_LINE_BYTES = RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES;

    private static final long DEFAULT_EXECUTOR_QUEUE_MAX_BYTES = 64L * 1024 * 1024;
    private static final int DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS = 1024;
    private static final long DEFAULT_TRANSACTION_QUEUE_MAX_BYTES = DEFAULT_EXECUTOR_QUEUE_MAX_BYTES;
    private static final long DEFAULT_BACKPRESSURE_BYTES_HIGH = 16L * 1024 * 1024;
    private static final long DEFAULT_BACKPRESSURE_BYTES_LOW = 8L * 1024 * 1024;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    public boolean help;

    @Option(names = ArgNames.PORT, defaultValue = "6378", description = "TCP port to bind.")
    public int port = 6378;

    @Option(names = ArgNames.DATABASES, defaultValue = "16", description = "Number of logical databases (SELECT 0..N-1).")
    public int databases = 16;

    @Option(
            names = ArgNames.CLEANUP_INTERVAL_MILLIS,
            defaultValue = "1000",
            description = "Expiration cleanup interval in milliseconds (0 disables cleanup)."
    )
    public long cleanupIntervalMillis = 1000;

    @Option(names = ArgNames.NO_CLEANUP, description = "Disable periodic expiration cleanup.")
    public boolean noCleanup;

    @Option(names = ArgNames.IO_THREADS, defaultValue = "1", description = "Netty I/O threads.")
    public int ioThreads = 1;

    @Option(names = ArgNames.EXECUTOR_QUEUE_CAPACITY, defaultValue = "1024", description = "Command executor queue capacity.")
    public int executorQueueCapacity = 1024;

    @Option(
            names = ArgNames.EXECUTOR_QUEUE_MAX_BYTES,
            defaultValue = "" + DEFAULT_EXECUTOR_QUEUE_MAX_BYTES,
            description = "Command executor queue max bytes (0 disables)."
    )
    public long executorQueueMaxBytes = DEFAULT_EXECUTOR_QUEUE_MAX_BYTES;

    @Option(
            names = ArgNames.EXECUTOR_SCHEDULING_POLICY,
            defaultValue = "fair",
            description = "Executor scheduling policy: global|fair."
    )
    public String executorSchedulingPolicy = "fair";

    @Option(names = ArgNames.BACKPRESSURE_HIGH, defaultValue = "256", description = "Backpressure high watermark.")
    public int backpressureHighWatermark = 256;

    @Option(names = ArgNames.BACKPRESSURE_LOW, defaultValue = "128", description = "Backpressure low watermark.")
    public int backpressureLowWatermark = 128;

    @Option(
            names = ArgNames.BACKPRESSURE_BYTES_HIGH,
            defaultValue = "" + DEFAULT_BACKPRESSURE_BYTES_HIGH,
            description = "Backpressure bytes high watermark (0 disables)."
    )
    public long backpressureBytesHighWatermark = DEFAULT_BACKPRESSURE_BYTES_HIGH;

    @Option(
            names = ArgNames.BACKPRESSURE_BYTES_LOW,
            defaultValue = "" + DEFAULT_BACKPRESSURE_BYTES_LOW,
            description = "Backpressure bytes low watermark (0 disables)."
    )
    public long backpressureBytesLowWatermark = DEFAULT_BACKPRESSURE_BYTES_LOW;

    @Option(names = ArgNames.EXECUTOR_MAX_DRAIN, defaultValue = "512", description = "Max commands drained per executor tick.")
    public int executorMaxDrainCommands = 512;

    @Option(names = ArgNames.EXECUTOR_DRAIN_MILLIS, defaultValue = "2", description = "Executor drain time budget in milliseconds.")
    public long executorDrainTimeLimitMillis = 2;

    @Option(
            names = ArgNames.TRANSACTION_QUEUE_MAX_COMMANDS,
            defaultValue = "" + DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS,
            description = "Transaction queue max commands for MULTI (0 disables)."
    )
    public int transactionQueueMaxCommands = DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS;

    @Option(
            names = ArgNames.TRANSACTION_QUEUE_MAX_BYTES,
            defaultValue = "" + DEFAULT_TRANSACTION_QUEUE_MAX_BYTES,
            description = "Transaction queue max bytes for MULTI (0 disables)."
    )
    public long transactionQueueMaxBytes = DEFAULT_TRANSACTION_QUEUE_MAX_BYTES;

    @Option(
            names = ArgNames.PROTOCOL_MAX_BULK_BYTES,
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_BULK_BYTES,
            description = "Protocol max request payload bytes."
    )
    public int protocolMaxBulkBytes = DEFAULT_PROTOCOL_MAX_BULK_BYTES;

    @Option(names = ArgNames.PROTOCOL_MAX_ARGS, defaultValue = "" + DEFAULT_PROTOCOL_MAX_ARGS, description = "Protocol max args per command.")
    public int protocolMaxArgs = DEFAULT_PROTOCOL_MAX_ARGS;

    @Option(
            names = ArgNames.PROTOCOL_MAX_LINE_BYTES,
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_LINE_BYTES,
            description = "Protocol max header bytes."
    )
    public int protocolMaxLineBytes = DEFAULT_PROTOCOL_MAX_LINE_BYTES;

    @Option(names = ArgNames.MAXMEMORY_BYTES, defaultValue = "0", description = "Maxmemory in bytes (0 disables eviction).")
    public long maxmemoryBytes = 0;

    @Option(names = ArgNames.MAXMEMORY_SCOPE, defaultValue = "global", description = "Maxmemory scope: global|per-db.")
    public String maxmemoryScope = "global";

    @Option(names = ArgNames.MAXMEMORY_POLICY, defaultValue = "noeviction", description = "Maxmemory policy string.")
    public String maxmemoryPolicy = "noeviction";

    @Option(names = ArgNames.MAXMEMORY_SAMPLES, defaultValue = "5", description = "Maxmemory samples (policy dependent).")
    public int maxmemorySamples = 5;

    @Option(names = ArgNames.EVICTION_TIME_LIMIT_MILLIS, defaultValue = "5", description = "Eviction time budget per tick in milliseconds.")
    public long evictionTimeLimitMillis = 5;

    @Option(names = ArgNames.EXPIRE_CLEANUP_TIME_LIMIT_MILLIS, defaultValue = "5", description = "Expire cleanup time budget per tick in milliseconds.")
    public long expireCleanupTimeLimitMillis = 5;

    @Option(names = ArgNames.NATIVE_DEFRAG_ENABLED, description = "Enable DB native allocator defrag during maintenance ticks.")
    public boolean nativeDefragEnabled;

    @Option(names = ArgNames.NATIVE_DEFRAG_MAX_MOVE_BYTES, defaultValue = "65536", description = "Native defrag max bytes to move per maintenance tick.")
    public long nativeDefragMaxMoveBytes = 64L * 1024L;

    @Option(names = ArgNames.NATIVE_DEFRAG_MAX_OBJECTS, defaultValue = "64", description = "Native defrag max objects to inspect per maintenance tick.")
    public long nativeDefragMaxObjects = 64L;

    @Option(names = ArgNames.NATIVE_DEFRAG_TIME_LIMIT_MILLIS, defaultValue = "1", description = "Native defrag time budget per maintenance tick in milliseconds.")
    public long nativeDefragTimeLimitMillis = 1L;

    @Option(names = ArgNames.NATIVE_SLOT_CAPACITY, defaultValue = "0", description = "Override DB shared native object slot capacity (0 keeps default).")
    public int nativeSlotCapacity;

    @Option(
            names = ArgNames.KEYS_TIME_BUDGET_MILLIS,
            defaultValue = "20",
            description = "KEYS time budget in milliseconds (0 disables; use SCAN for large datasets)."
    )
    public long keysTimeBudgetMillis = 20;

    @Option(
            names = ArgNames.KEYS_MAX_RESULTS,
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

    public YierdisBenchServerArgs copy() {
        YierdisBenchServerArgs out = new YierdisBenchServerArgs();
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

    public List<String> toArgv() {
        List<String> out = new ArrayList<>();
        if (help) {
            out.add("--help");
            return out;
        }

        out.add(ArgNames.PORT);
        out.add(Integer.toString(port));
        out.add(ArgNames.DATABASES);
        out.add(Integer.toString(databases));
        if (noCleanup) {
            out.add(ArgNames.NO_CLEANUP);
        } else {
            out.add(ArgNames.CLEANUP_INTERVAL_MILLIS);
            out.add(Long.toString(cleanupIntervalMillis));
        }
        out.add(ArgNames.IO_THREADS);
        out.add(Integer.toString(ioThreads));
        out.add(ArgNames.EXECUTOR_QUEUE_CAPACITY);
        out.add(Integer.toString(executorQueueCapacity));
        out.add(ArgNames.EXECUTOR_QUEUE_MAX_BYTES);
        out.add(Long.toString(executorQueueMaxBytes));
        out.add(ArgNames.EXECUTOR_SCHEDULING_POLICY);
        out.add(executorSchedulingPolicy);
        out.add(ArgNames.BACKPRESSURE_HIGH);
        out.add(Integer.toString(backpressureHighWatermark));
        out.add(ArgNames.BACKPRESSURE_LOW);
        out.add(Integer.toString(backpressureLowWatermark));
        out.add(ArgNames.BACKPRESSURE_BYTES_HIGH);
        out.add(Long.toString(backpressureBytesHighWatermark));
        out.add(ArgNames.BACKPRESSURE_BYTES_LOW);
        out.add(Long.toString(backpressureBytesLowWatermark));
        out.add(ArgNames.EXECUTOR_MAX_DRAIN);
        out.add(Integer.toString(executorMaxDrainCommands));
        out.add(ArgNames.EXECUTOR_DRAIN_MILLIS);
        out.add(Long.toString(executorDrainTimeLimitMillis));
        out.add(ArgNames.TRANSACTION_QUEUE_MAX_COMMANDS);
        out.add(Integer.toString(transactionQueueMaxCommands));
        out.add(ArgNames.TRANSACTION_QUEUE_MAX_BYTES);
        out.add(Long.toString(transactionQueueMaxBytes));
        out.add(ArgNames.PROTOCOL_MAX_BULK_BYTES);
        out.add(Integer.toString(protocolMaxBulkBytes));
        out.add(ArgNames.PROTOCOL_MAX_ARGS);
        out.add(Integer.toString(protocolMaxArgs));
        out.add(ArgNames.PROTOCOL_MAX_LINE_BYTES);
        out.add(Integer.toString(protocolMaxLineBytes));
        out.add(ArgNames.MAXMEMORY_BYTES);
        out.add(Long.toString(maxmemoryBytes));
        out.add(ArgNames.MAXMEMORY_SCOPE);
        out.add(maxmemoryScope);
        out.add(ArgNames.MAXMEMORY_POLICY);
        out.add(maxmemoryPolicy);
        out.add(ArgNames.MAXMEMORY_SAMPLES);
        out.add(Integer.toString(maxmemorySamples));
        out.add(ArgNames.EVICTION_TIME_LIMIT_MILLIS);
        out.add(Long.toString(evictionTimeLimitMillis));
        out.add(ArgNames.EXPIRE_CLEANUP_TIME_LIMIT_MILLIS);
        out.add(Long.toString(expireCleanupTimeLimitMillis));
        if (nativeDefragEnabled) {
            out.add(ArgNames.NATIVE_DEFRAG_ENABLED);
        }
        out.add(ArgNames.NATIVE_DEFRAG_MAX_MOVE_BYTES);
        out.add(Long.toString(nativeDefragMaxMoveBytes));
        out.add(ArgNames.NATIVE_DEFRAG_MAX_OBJECTS);
        out.add(Long.toString(nativeDefragMaxObjects));
        out.add(ArgNames.NATIVE_DEFRAG_TIME_LIMIT_MILLIS);
        out.add(Long.toString(nativeDefragTimeLimitMillis));
        out.add(ArgNames.NATIVE_SLOT_CAPACITY);
        out.add(Integer.toString(nativeSlotCapacity));
        out.add(ArgNames.KEYS_TIME_BUDGET_MILLIS);
        out.add(Long.toString(keysTimeBudgetMillis));
        out.add(ArgNames.KEYS_MAX_RESULTS);
        out.add(Integer.toString(keysMaxResults));
        return out;
    }

    private static String normalizeExecutorSchedulingPolicy(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("executorSchedulingPolicy must not be blank");
        }
        String normalized = rawValue.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "global", "fair" -> normalized;
            default -> throw new IllegalArgumentException("unsupported executorSchedulingPolicy: " + rawValue);
        };
    }

    private static String normalizeMaxmemoryScope(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("maxmemoryScope must not be blank");
        }
        String normalized = rawValue.trim().toLowerCase(Locale.ROOT).replace('_', '-');
        if ("perdb".equals(normalized)) {
            return "per-db";
        }
        return switch (normalized) {
            case "global", "per-db" -> normalized;
            default -> throw new IllegalArgumentException("unsupported maxmemoryScope: " + rawValue);
        };
    }

    private static String normalizeMaxmemoryPolicy(String rawValue) {
        return MaxmemoryPolicy.parse(rawValue).redisName();
    }

    private static final class ArgNames {
        private static final String PORT = "--port";
        private static final String DATABASES = "--databases";
        private static final String CLEANUP_INTERVAL_MILLIS = "--cleanupIntervalMillis";
        private static final String NO_CLEANUP = "--noCleanup";
        private static final String IO_THREADS = "--ioThreads";
        private static final String EXECUTOR_QUEUE_CAPACITY = "--executorQueueCapacity";
        private static final String EXECUTOR_QUEUE_MAX_BYTES = "--executorQueueMaxBytes";
        private static final String EXECUTOR_SCHEDULING_POLICY = "--executorSchedulingPolicy";
        private static final String BACKPRESSURE_HIGH = "--backpressureHigh";
        private static final String BACKPRESSURE_LOW = "--backpressureLow";
        private static final String BACKPRESSURE_BYTES_HIGH = "--backpressureBytesHigh";
        private static final String BACKPRESSURE_BYTES_LOW = "--backpressureBytesLow";
        private static final String EXECUTOR_MAX_DRAIN = "--executorMaxDrain";
        private static final String EXECUTOR_DRAIN_MILLIS = "--executorDrainMillis";
        private static final String TRANSACTION_QUEUE_MAX_COMMANDS = "--transactionQueueMaxCommands";
        private static final String TRANSACTION_QUEUE_MAX_BYTES = "--transactionQueueMaxBytes";
        private static final String PROTOCOL_MAX_BULK_BYTES = "--protocolMaxBulkBytes";
        private static final String PROTOCOL_MAX_ARGS = "--protocolMaxArgs";
        private static final String PROTOCOL_MAX_LINE_BYTES = "--protocolMaxLineBytes";
        private static final String MAXMEMORY_BYTES = "--maxmemoryBytes";
        private static final String MAXMEMORY_SCOPE = "--maxmemoryScope";
        private static final String MAXMEMORY_POLICY = "--maxmemoryPolicy";
        private static final String MAXMEMORY_SAMPLES = "--maxmemorySamples";
        private static final String EVICTION_TIME_LIMIT_MILLIS = "--evictionTimeLimitMillis";
        private static final String EXPIRE_CLEANUP_TIME_LIMIT_MILLIS = "--expireCleanupTimeLimitMillis";
        private static final String NATIVE_DEFRAG_ENABLED = "--nativeDefragEnabled";
        private static final String NATIVE_DEFRAG_MAX_MOVE_BYTES = "--nativeDefragMaxMoveBytes";
        private static final String NATIVE_DEFRAG_MAX_OBJECTS = "--nativeDefragMaxObjects";
        private static final String NATIVE_DEFRAG_TIME_LIMIT_MILLIS = "--nativeDefragTimeLimitMillis";
        private static final String NATIVE_SLOT_CAPACITY = "--nativeSlotCapacity";
        private static final String KEYS_TIME_BUDGET_MILLIS = "--keysTimeBudgetMillis";
        private static final String KEYS_MAX_RESULTS = "--keysMaxResults";

        private ArgNames() {
        }
    }
}
