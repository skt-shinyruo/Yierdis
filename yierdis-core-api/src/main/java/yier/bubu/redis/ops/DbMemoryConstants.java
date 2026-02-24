package yier.bubu.redis.ops;

// 内存口径常量：确保 DB accounting 与命令层 prepareWrite 的估算一致，避免双常量漂移导致 maxmemory 行为难以解释。

public final class DbMemoryConstants {
    /**
     * Best-effort per-entry overhead estimate.
     * <p>
     * 说明：JVM 对象头、指针与对齐等真实开销远大于 payload bytes；该值用于让 maxmemory enforcement 更接近真实堆压。
     */
    public static final long ENTRY_OVERHEAD_BYTES_ESTIMATE = 64L;

    private DbMemoryConstants() {
    }
}
