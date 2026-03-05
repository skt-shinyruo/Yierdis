package yier.bubu.redis.ops;

import java.util.Objects;

/**
 * Eviction candidate sampled from a {@link MaxmemoryParticipant}.
 * <p>
 * {@code key} is a {@code byte[]} for engine-specific encoding/performance reasons. Callers MUST treat the
 * array as immutable (do not modify its contents), and should not assume it remains valid after the
 * eviction attempt completes unless the participant explicitly documents a longer lifetime.
 * <p>
 * NOTE: Record-generated {@code equals}/{@code hashCode} are NOT content-based for {@code byte[]} (array
 * equality is reference-based). Do not rely on {@link MaxmemoryCandidate} as a value object or as a map key
 * unless you provide your own content-based comparison.
 *
 * @param owner    candidate owner (participant)
 * @param key      key bytes (engine-specific encoding); must be treated as immutable; lifecycle is owned by {@code owner}
 * @param lruClock global or engine-local lru clock (meaningful for LRU policy)
 */
public record MaxmemoryCandidate(MaxmemoryParticipant owner, byte[] key, long lruClock) {
    public MaxmemoryCandidate {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(key, "key");
    }
}
