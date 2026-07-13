package yier.bubu.redis.storage.memory.internal.hash;

public record HashTableMetrics(
        int capacity,
        int size,
        int filledSlots,
        int tombstones,
        boolean rehashing,
        int oldCapacity,
        int rehashCursor,
        long generation,
        long completedRehashes,
        int maximumProbeLength
) {
}
