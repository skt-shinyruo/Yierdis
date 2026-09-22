package yier.bubu.redis.storage.memory.internal.value;

/**
 * Redis 风格 index 区间归一化：负值按尾部计数，越界收敛到集合边界，空区间返回 {@code null}。
 * 调用方只需在 {@code null} 时返回各自的空结果。
 */
record NormalizedRange(long start, long stop) {
    static NormalizedRange of(int size, long start, long stop) {
        if (size == 0) {
            return null;
        }
        long normalizedStart = start >= 0 ? start : (long) size + start;
        long normalizedStop = stop >= 0 ? stop : (long) size + stop;
        if (normalizedStart < 0) {
            normalizedStart = 0;
        }
        if (normalizedStop < 0) {
            return null;
        }
        if (normalizedStop >= size) {
            normalizedStop = size - 1;
        }
        if (normalizedStart > normalizedStop) {
            return null;
        }
        return new NormalizedRange(normalizedStart, normalizedStop);
    }
}
