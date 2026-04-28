package yier.bubu.redis.ops;

import java.util.Objects;

/**
 * Eviction candidate sampled from a {@link MaxmemoryParticipant}.
 * <p>
 * {@code keyHandle} is an opaque participant-owned key identity. It may point to heap or off-heap storage,
 * and callers should not assume it remains valid after the eviction attempt completes unless the participant
 * explicitly documents a longer lifetime.
 *
 * @param owner    candidate owner (participant)
 * @param keyHandle key identity handle; lifecycle is owned by {@code owner}
 * @param lruClock global or engine-local lru clock (meaningful for LRU policy)
 */
public record MaxmemoryCandidate(MaxmemoryParticipant owner, KeyHandle keyHandle, long lruClock) {
    public MaxmemoryCandidate {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(keyHandle, "keyHandle");
    }
}
