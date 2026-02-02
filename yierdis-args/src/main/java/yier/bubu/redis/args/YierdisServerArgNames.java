package yier.bubu.redis.args;

public final class YierdisServerArgNames {
    public static final String PORT = "--port";
    public static final String DATABASES = "--databases";
    public static final String CLEANUP_INTERVAL_MILLIS = "--cleanupIntervalMillis";
    public static final String NO_CLEANUP = "--noCleanup";
    public static final String IO_THREADS = "--ioThreads";
    public static final String EXECUTOR_QUEUE_CAPACITY = "--executorQueueCapacity";
    public static final String EXECUTOR_QUEUE_MAX_BYTES = "--executorQueueMaxBytes";
    public static final String EXECUTOR_SCHEDULING_POLICY = "--executorSchedulingPolicy";
    public static final String FRAME_COMPACTION_THRESHOLD_BYTES = "--frameCompactionThresholdBytes";
    public static final String FRAME_COMPACTION_RATIO = "--frameCompactionRatio";
    public static final String FRAME_COMPACTION_MAX_COPY_BYTES = "--frameCompactionMaxCopyBytes";
    public static final String BACKPRESSURE_HIGH = "--backpressureHigh";
    public static final String BACKPRESSURE_LOW = "--backpressureLow";
    public static final String BACKPRESSURE_BYTES_HIGH = "--backpressureBytesHigh";
    public static final String BACKPRESSURE_BYTES_LOW = "--backpressureBytesLow";
    public static final String EXECUTOR_MAX_DRAIN = "--executorMaxDrain";
    public static final String EXECUTOR_DRAIN_MILLIS = "--executorDrainMillis";

    public static final String PROTOCOL_MAX_BULK_BYTES = "--protocolMaxBulkBytes";
    public static final String PROTOCOL_MAX_ARGS = "--protocolMaxArgs";
    public static final String PROTOCOL_MAX_LINE_BYTES = "--protocolMaxLineBytes";

    public static final String OFFHEAP_BACKEND = "--offheapBackend";
    public static final String OFFHEAP_MAX_BYTES = "--offheapMaxBytes";
    public static final String OFFHEAP_KEYS_ENABLED = "--offheapKeysEnabled";
    public static final String MAXMEMORY_BYTES = "--maxmemoryBytes";
    public static final String MAXMEMORY_POLICY = "--maxmemoryPolicy";
    public static final String MAXMEMORY_SAMPLES = "--maxmemorySamples";
    public static final String EVICTION_TIME_LIMIT_MILLIS = "--evictionTimeLimitMillis";
    public static final String EXPIRE_CLEANUP_TIME_LIMIT_MILLIS = "--expireCleanupTimeLimitMillis";

    private YierdisServerArgNames() {
    }
}
