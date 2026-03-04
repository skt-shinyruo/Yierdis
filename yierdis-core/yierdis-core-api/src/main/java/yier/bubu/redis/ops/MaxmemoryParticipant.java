package yier.bubu.redis.ops;

/**
 * Participant view used by a global maxmemory governor.
 */
public interface MaxmemoryParticipant {
    /**
     * Dataset usage for maxmemory accounting, excluding shared usage sources.
     */
    long usedBytesForMaxmemory();

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

