package yier.bubu.redis.args;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import yier.bubu.redis.protocol.RespLimits;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Command(
        name = "yierdis",
        description = "A simplified Redis-compatible server (teaching-oriented).",
        sortOptions = false,
        usageHelpAutoWidth = true
)
public final class YierdisServerArgs {
    private static final int DEFAULT_PROTOCOL_MAX_BULK_BYTES = RespLimits.DEFAULT_MAX_BULK_BYTES;
    private static final int DEFAULT_PROTOCOL_MAX_ARGS = RespLimits.DEFAULT_MAX_ARGS;
    private static final int DEFAULT_PROTOCOL_MAX_LINE_BYTES = RespLimits.DEFAULT_MAX_LINE_BYTES;

    private static final long DEFAULT_EXECUTOR_QUEUE_MAX_BYTES = 64L * 1024 * 1024; // 64 MiB
    private static final long DEFAULT_BACKPRESSURE_BYTES_HIGH = 16L * 1024 * 1024; // 16 MiB
    private static final long DEFAULT_BACKPRESSURE_BYTES_LOW = 8L * 1024 * 1024; // 8 MiB
    private static final int DEFAULT_FRAME_COMPACTION_MAX_COPY_BYTES = 1024 * 1024; // 1 MiB

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

    @Option(
            names = YierdisServerArgNames.FRAME_COMPACTION_THRESHOLD_BYTES,
            defaultValue = "0",
            description = "Frame compaction threshold bytes (0 disables)."
    )
    public long frameCompactionThresholdBytes = 0;

    @Option(
            names = YierdisServerArgNames.FRAME_COMPACTION_RATIO,
            defaultValue = "2.0",
            description = "Frame compaction retained/length ratio threshold (>= 1.0)."
    )
    public double frameCompactionRatio = 2.0;

    @Option(
            names = YierdisServerArgNames.FRAME_COMPACTION_MAX_COPY_BYTES,
            defaultValue = "" + DEFAULT_FRAME_COMPACTION_MAX_COPY_BYTES,
            description = "Frame compaction max copy bytes."
    )
    public int frameCompactionMaxCopyBytes = DEFAULT_FRAME_COMPACTION_MAX_COPY_BYTES;

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
            names = YierdisServerArgNames.PROTOCOL_MAX_BULK_BYTES,
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_BULK_BYTES,
            description = "Protocol max bulk string bytes."
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
            description = "Protocol max line bytes."
    )
    public int protocolMaxLineBytes = DEFAULT_PROTOCOL_MAX_LINE_BYTES;

    @Option(
            names = YierdisServerArgNames.OFFHEAP_BACKEND,
            defaultValue = "none",
            description = "Off-heap backend: none|netty|unsafe|foreign."
    )
    public String offheapBackend = "none";

    @Option(
            names = YierdisServerArgNames.OFFHEAP_MAX_BYTES,
            defaultValue = "0",
            description = "Off-heap max bytes (only valid when offheap backend is not 'none')."
    )
    public long offheapMaxBytes = 0;

    @Option(names = YierdisServerArgNames.OFFHEAP_KEYS_ENABLED, description = "Enable storing keys/expires in off-heap (unsafe backend only).")
    public boolean offheapKeysEnabled;

    @Option(names = YierdisServerArgNames.MAXMEMORY_BYTES, defaultValue = "0", description = "Maxmemory in bytes (0 disables eviction).")
    public long maxmemoryBytes = 0;

    @Option(names = YierdisServerArgNames.MAXMEMORY_POLICY, defaultValue = "noeviction", description = "Maxmemory policy string.")
    public String maxmemoryPolicy = "noeviction";

    @Option(names = YierdisServerArgNames.MAXMEMORY_SAMPLES, defaultValue = "5", description = "Maxmemory samples (policy dependent).")
    public int maxmemorySamples = 5;

    @Option(names = YierdisServerArgNames.EVICTION_TIME_LIMIT_MILLIS, defaultValue = "5", description = "Eviction time budget per tick in milliseconds.")
    public long evictionTimeLimitMillis = 5;

    @Option(names = YierdisServerArgNames.EXPIRE_CLEANUP_TIME_LIMIT_MILLIS, defaultValue = "5", description = "Expire cleanup time budget per tick in milliseconds.")
    public long expireCleanupTimeLimitMillis = 5;

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
        if (executorSchedulingPolicy == null || executorSchedulingPolicy.isBlank()) {
            throw new IllegalArgumentException("executorSchedulingPolicy must not be blank");
        }
        String executorPolicy = executorSchedulingPolicy.trim().toLowerCase(Locale.ROOT);
        if (!executorPolicy.equals("global") && !executorPolicy.equals("fair")) {
            throw new IllegalArgumentException("unsupported executorSchedulingPolicy: " + executorSchedulingPolicy);
        }
        executorSchedulingPolicy = executorPolicy;
        if (frameCompactionThresholdBytes < 0) {
            throw new IllegalArgumentException("frameCompactionThresholdBytes must be >= 0");
        }
        if (Double.isNaN(frameCompactionRatio) || frameCompactionRatio < 1.0) {
            throw new IllegalArgumentException("frameCompactionRatio must be >= 1.0");
        }
        if (frameCompactionMaxCopyBytes <= 0) {
            throw new IllegalArgumentException("frameCompactionMaxCopyBytes must be > 0");
        }
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
        if (protocolMaxBulkBytes <= 0) {
            throw new IllegalArgumentException("protocolMaxBulkBytes must be > 0");
        }
        if (protocolMaxArgs <= 0) {
            throw new IllegalArgumentException("protocolMaxArgs must be > 0");
        }
        if (protocolMaxLineBytes <= 0) {
            throw new IllegalArgumentException("protocolMaxLineBytes must be > 0");
        }
        if (offheapBackend == null || offheapBackend.isBlank()) {
            throw new IllegalArgumentException("offheapBackend must not be blank");
        }

        String backend = offheapBackend.trim().toLowerCase(Locale.ROOT);
        if (!backend.equals("none") && !backend.equals("netty") && !backend.equals("unsafe") && !backend.equals("foreign")) {
            throw new IllegalArgumentException("unsupported offheapBackend: " + offheapBackend);
        }
        offheapBackend = backend;

        if (offheapMaxBytes < 0) {
            throw new IllegalArgumentException("offheapMaxBytes must be >= 0");
        }
        if (backend.equals("none") && offheapMaxBytes != 0) {
            throw new IllegalArgumentException("offheapMaxBytes must be 0 when offheapBackend is 'none'");
        }
        if (offheapKeysEnabled && !backend.equals("unsafe")) {
            throw new IllegalArgumentException("offheapKeysEnabled requires offheapBackend='unsafe'");
        }
        if (maxmemoryBytes < 0) {
            throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
        }
        if (maxmemoryPolicy == null || maxmemoryPolicy.isBlank()) {
            throw new IllegalArgumentException("maxmemoryPolicy must not be blank");
        }
        String policy = maxmemoryPolicy.trim().toLowerCase(Locale.ROOT);
        if (!policy.equals("noeviction") && !policy.equals("allkeys-random") && !policy.equals("allkeys-lru")) {
            throw new IllegalArgumentException("unsupported maxmemoryPolicy: " + maxmemoryPolicy);
        }
        maxmemoryPolicy = policy;
        if (maxmemorySamples <= 0) {
            throw new IllegalArgumentException("maxmemorySamples must be > 0");
        }
        if (evictionTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("evictionTimeLimitMillis must be > 0");
        }
        if (expireCleanupTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("expireCleanupTimeLimitMillis must be > 0");
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
        out.frameCompactionThresholdBytes = frameCompactionThresholdBytes;
        out.frameCompactionRatio = frameCompactionRatio;
        out.frameCompactionMaxCopyBytes = frameCompactionMaxCopyBytes;
        out.backpressureHighWatermark = backpressureHighWatermark;
        out.backpressureLowWatermark = backpressureLowWatermark;
        out.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        out.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        out.executorMaxDrainCommands = executorMaxDrainCommands;
        out.executorDrainTimeLimitMillis = executorDrainTimeLimitMillis;
        out.protocolMaxBulkBytes = protocolMaxBulkBytes;
        out.protocolMaxArgs = protocolMaxArgs;
        out.protocolMaxLineBytes = protocolMaxLineBytes;
        out.offheapBackend = offheapBackend;
        out.offheapMaxBytes = offheapMaxBytes;
        out.offheapKeysEnabled = offheapKeysEnabled;
        out.maxmemoryBytes = maxmemoryBytes;
        out.maxmemoryPolicy = maxmemoryPolicy;
        out.maxmemorySamples = maxmemorySamples;
        out.evictionTimeLimitMillis = evictionTimeLimitMillis;
        out.expireCleanupTimeLimitMillis = expireCleanupTimeLimitMillis;
        return out;
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
        out.add(YierdisServerArgNames.FRAME_COMPACTION_THRESHOLD_BYTES);
        out.add(Long.toString(frameCompactionThresholdBytes));
        out.add(YierdisServerArgNames.FRAME_COMPACTION_RATIO);
        out.add(Double.toString(frameCompactionRatio));
        out.add(YierdisServerArgNames.FRAME_COMPACTION_MAX_COPY_BYTES);
        out.add(Integer.toString(frameCompactionMaxCopyBytes));
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

        out.add(YierdisServerArgNames.PROTOCOL_MAX_BULK_BYTES);
        out.add(Integer.toString(protocolMaxBulkBytes));
        out.add(YierdisServerArgNames.PROTOCOL_MAX_ARGS);
        out.add(Integer.toString(protocolMaxArgs));
        out.add(YierdisServerArgNames.PROTOCOL_MAX_LINE_BYTES);
        out.add(Integer.toString(protocolMaxLineBytes));

        out.add(YierdisServerArgNames.OFFHEAP_BACKEND);
        out.add(offheapBackend);
        out.add(YierdisServerArgNames.OFFHEAP_MAX_BYTES);
        out.add(Long.toString(offheapMaxBytes));
        if (offheapKeysEnabled) {
            out.add(YierdisServerArgNames.OFFHEAP_KEYS_ENABLED);
        }

        out.add(YierdisServerArgNames.MAXMEMORY_BYTES);
        out.add(Long.toString(maxmemoryBytes));
        out.add(YierdisServerArgNames.MAXMEMORY_POLICY);
        out.add(maxmemoryPolicy);
        out.add(YierdisServerArgNames.MAXMEMORY_SAMPLES);
        out.add(Integer.toString(maxmemorySamples));

        out.add(YierdisServerArgNames.EVICTION_TIME_LIMIT_MILLIS);
        out.add(Long.toString(evictionTimeLimitMillis));
        out.add(YierdisServerArgNames.EXPIRE_CLEANUP_TIME_LIMIT_MILLIS);
        out.add(Long.toString(expireCleanupTimeLimitMillis));

        return out;
    }
}
