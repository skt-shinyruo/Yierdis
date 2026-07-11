package yier.bubu.redis.memory.foreign;

import java.util.concurrent.atomic.AtomicReference;

final class YierdisAllocatorThreadGuard {
    private final boolean enabled;
    private final AtomicReference<Thread> ownerThread = new AtomicReference<>();

    YierdisAllocatorThreadGuard(boolean enabled) {
        this.enabled = enabled;
    }

    void bindToCurrentThread() {
        checkOrBindCurrentThread();
    }

    void checkOrBindCurrentThread() {
        if (!enabled) {
            return;
        }
        Thread current = Thread.currentThread();
        Thread owner = ownerThread.get();
        if (owner == current) {
            return;
        }
        if (owner == null && ownerThread.compareAndSet(null, current)) {
            return;
        }
        throw new IllegalStateException("stable native allocator accessed from non-owner thread");
    }
}
