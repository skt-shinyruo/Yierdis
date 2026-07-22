package yier.bubu.redis.memory.api;

import java.util.concurrent.atomic.AtomicLong;

final class StableMemoryBackendIdSequence {
    private final AtomicLong next;

    StableMemoryBackendIdSequence(long initialId) {
        if (initialId <= 0L) {
            throw new IllegalArgumentException("initialId must be positive");
        }
        this.next = new AtomicLong(initialId);
    }

    long nextId() {
        for (;;) {
            long current = next.get();
            if (current <= 0L) {
                throw new IllegalStateException("stable memory backend IDs are exhausted");
            }
            long successor = current == Long.MAX_VALUE ? 0L : current + 1L;
            if (next.compareAndSet(current, successor)) {
                return current;
            }
        }
    }
}
