package yier.bubu.redis.db.memory.offheap;

// OffHeapKeyCopyDiagnostics：用于测试与诊断“off-heap keys 是否发生 canonical heap copy”。

import java.util.concurrent.atomic.LongAdder;

/**
 * 诊断：统计从 off-heap keyspace/expire index 复制 key bytes 到 heap 的次数。
 * <p>
 * 该计数仅用于回归测试与性能诊断：
 * - 当 key 已存储在 off-heap 时，读路径（GET/EXISTS/TYPE/TTL 等）应避免产生 canonical heap key copy。
 * - KEYS/SCAN/随机采样等需要返回 heap byte[] 的路径可能会产生拷贝，这里用于观测与对比。
 */
public final class OffHeapKeyCopyDiagnostics {
    private static final LongAdder HEAP_KEY_COPIES = new LongAdder();

    private OffHeapKeyCopyDiagnostics() {
    }

    public static void onHeapKeyCopy() {
        HEAP_KEY_COPIES.increment();
    }

    public static long heapKeyCopies() {
        return HEAP_KEY_COPIES.sum();
    }

    public static void reset() {
        HEAP_KEY_COPIES.reset();
    }
}

