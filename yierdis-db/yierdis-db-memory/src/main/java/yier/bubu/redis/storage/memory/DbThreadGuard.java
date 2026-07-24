package yier.bubu.redis.storage.memory;

import java.util.concurrent.atomic.AtomicReference;
import yier.bubu.redis.memory.api.MemoryOwner;

public final class DbThreadGuard implements MemoryOwner {
    private enum Lifecycle {
        OPEN,
        CLOSING,
        CLOSED
    }

    private final AtomicReference<Thread> owner = new AtomicReference<>();
    private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>(Lifecycle.OPEN);

    @Override
    public void bindToCurrentThread() {
        requireOpen();
        Thread current = Thread.currentThread();
        Thread existing = owner.get();
        if (existing == current) {
            return;
        }
        if (existing != null || !owner.compareAndSet(null, current)) {
            throw new IllegalStateException("YierdisDb is already bound to another thread");
        }
    }

    void checkDbAccess() {
        requireOpen();
        requireOwner(
                "YierdisDb accessed before bindToCurrentThread()",
                "YierdisDb accessed from a non-owner thread"
        );
    }

    @Override
    public void checkCurrentThread() {
        if (lifecycle.get() == Lifecycle.CLOSED) {
            throw new IllegalStateException("YierdisDb is CLOSED");
        }
        requireOwner(
                "memory accessed before bindToCurrentThread()",
                "memory accessed from a non-owner thread"
        );
    }

    @Override
    public void checkCurrentThreadForShutdown() {
        Thread existing = owner.get();
        if (existing != null && existing != Thread.currentThread()) {
            throw new IllegalStateException("YierdisDb shutdown from a non-owner thread");
        }
    }

    boolean beginShutdown() {
        checkCurrentThreadForShutdown();
        return lifecycle.compareAndSet(Lifecycle.OPEN, Lifecycle.CLOSING);
    }

    void finishShutdown() {
        lifecycle.set(Lifecycle.CLOSED);
    }

    private void requireOpen() {
        Lifecycle current = lifecycle.get();
        if (current != Lifecycle.OPEN) {
            throw new IllegalStateException("YierdisDb is " + current);
        }
    }

    private void requireOwner(String unboundMessage, String foreignMessage) {
        Thread existing = owner.get();
        if (existing == null) {
            throw new IllegalStateException(unboundMessage);
        }
        if (existing != Thread.currentThread()) {
            throw new IllegalStateException(foreignMessage);
        }
    }
}
