package yier.bubu.redis.ops;

import java.util.Objects;

/**
 * Eviction candidate sampled from a {@link MaxmemoryParticipant}.
 *
 * @param owner    candidate owner (participant)
 * @param key      key bytes (engine-specific encoding)
 * @param lruClock global or engine-local lru clock (meaningful for LRU policy)
 */
public record MaxmemoryCandidate(MaxmemoryParticipant owner, byte[] key, long lruClock) {
    public MaxmemoryCandidate {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(key, "key");
    }
}

