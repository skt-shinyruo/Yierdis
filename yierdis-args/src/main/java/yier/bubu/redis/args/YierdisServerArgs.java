package yier.bubu.redis.args;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

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
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    public boolean help;

    @Option(names = YierdisServerArgNames.PORT, defaultValue = "6378", description = "TCP port to bind.")
    public int port = 6378;

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

    @Option(names = YierdisServerArgNames.BACKPRESSURE_HIGH, defaultValue = "256", description = "Backpressure high watermark.")
    public int backpressureHighWatermark = 256;

    @Option(names = YierdisServerArgNames.BACKPRESSURE_LOW, defaultValue = "128", description = "Backpressure low watermark.")
    public int backpressureLowWatermark = 128;

    @Option(names = YierdisServerArgNames.EXECUTOR_MAX_DRAIN, defaultValue = "512", description = "Max commands drained per executor tick.")
    public int executorMaxDrainCommands = 512;

    @Option(names = YierdisServerArgNames.EXECUTOR_DRAIN_MILLIS, defaultValue = "2", description = "Executor drain time budget in milliseconds.")
    public long executorDrainTimeLimitMillis = 2;

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

        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be in range 1..65535");
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
        if (backpressureHighWatermark <= 0) {
            throw new IllegalArgumentException("backpressureHighWatermark must be > 0");
        }
        if (backpressureLowWatermark < 0) {
            throw new IllegalArgumentException("backpressureLowWatermark must be >= 0");
        }
        if (backpressureLowWatermark >= backpressureHighWatermark) {
            throw new IllegalArgumentException("backpressureLowWatermark must be < backpressureHighWatermark");
        }
        if (executorMaxDrainCommands <= 0) {
            throw new IllegalArgumentException("executorMaxDrainCommands must be > 0");
        }
        if (executorDrainTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("executorDrainTimeLimitMillis must be > 0");
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
        out.cleanupIntervalMillis = cleanupIntervalMillis;
        out.noCleanup = noCleanup;
        out.ioThreads = ioThreads;
        out.executorQueueCapacity = executorQueueCapacity;
        out.backpressureHighWatermark = backpressureHighWatermark;
        out.backpressureLowWatermark = backpressureLowWatermark;
        out.executorMaxDrainCommands = executorMaxDrainCommands;
        out.executorDrainTimeLimitMillis = executorDrainTimeLimitMillis;
        out.offheapBackend = offheapBackend;
        out.offheapMaxBytes = offheapMaxBytes;
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
        out.add(YierdisServerArgNames.BACKPRESSURE_HIGH);
        out.add(Integer.toString(backpressureHighWatermark));
        out.add(YierdisServerArgNames.BACKPRESSURE_LOW);
        out.add(Integer.toString(backpressureLowWatermark));
        out.add(YierdisServerArgNames.EXECUTOR_MAX_DRAIN);
        out.add(Integer.toString(executorMaxDrainCommands));
        out.add(YierdisServerArgNames.EXECUTOR_DRAIN_MILLIS);
        out.add(Long.toString(executorDrainTimeLimitMillis));

        out.add(YierdisServerArgNames.OFFHEAP_BACKEND);
        out.add(offheapBackend);
        out.add(YierdisServerArgNames.OFFHEAP_MAX_BYTES);
        out.add(Long.toString(offheapMaxBytes));

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
