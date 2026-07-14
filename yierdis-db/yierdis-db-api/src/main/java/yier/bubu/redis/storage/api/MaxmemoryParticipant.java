package yier.bubu.redis.storage.api;

/**
 * 全局 maxmemory governor 使用的参与者视图。
 *
 * 每个参与者必须报告其独占的物理内存快照；全局 governor 将各快照相加，不能再补充共享运行时计数。
 */
public interface MaxmemoryParticipant extends MemoryUsageParticipant {
    /**
     * 兼容旧调用方的 maxmemory 使用量投影。
     */
    default long usedBytesForMaxmemory() {
        return memoryUsage().effectiveBytesForMaxmemory();
    }

    /**
     * Returns a non-negative estimate of evictable keys currently held by this participant.
     * <p>
     * This value may be approximate and is allowed to saturate at {@link Integer#MAX_VALUE}.
     */
    int keyCountEstimate();

    /**
     * Best-effort expired key cleanup to reduce eviction work.
     */
    void cleanupExpired(long nowMillis);

    /**
     * Sample an eviction candidate under the specified policy.
     *
     * @return a candidate, or {@code null} if no candidate exists
     */
    MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis);

    /**
     * Optional deterministic full scan for best candidate selection (e.g. true LRU).
     *
     * @return best candidate, or {@code null} if unsupported / no candidates
     */
    default MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
        return null;
    }

    /**
     * Evict a key described by {@code candidate}.
     *
     * @return true if eviction occurred (key deleted), false otherwise
     */
    boolean evict(MaxmemoryCandidate candidate, long nowMillis);
}
