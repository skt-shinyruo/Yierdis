package yier.bubu.redis.execution.api;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

/**
 * 线程安全的请求租约，在最后一个保留视图关闭时归还一次完整额度。
 */
public final class ReferenceCountedRequestMemoryLease implements RequestMemoryLease {
    private final State state;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ReferenceCountedRequestMemoryLease(long reservedBytes, LongConsumer onFinalRelease) {
        if (reservedBytes < 0L) {
            throw new IllegalArgumentException("reservedBytes must be non-negative");
        }
        this.state = new State(reservedBytes, Objects.requireNonNull(onFinalRelease, "onFinalRelease"));
    }

    private ReferenceCountedRequestMemoryLease(State state) {
        this.state = state;
    }

    @Override
    public long reservedBytes() {
        return state.reservedBytes;
    }

    @Override
    public boolean released() {
        return state.references.get() == 0;
    }

    @Override
    public RequestMemoryLease retain() {
        if (closed.get()) {
            throw new IllegalStateException("request memory lease view is closed");
        }
        state.retain();
        return new ReferenceCountedRequestMemoryLease(state);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            state.release();
        }
    }

    private static final class State {
        private final long reservedBytes;
        private final LongConsumer onFinalRelease;
        private final AtomicInteger references = new AtomicInteger(1);

        private State(long reservedBytes, LongConsumer onFinalRelease) {
            this.reservedBytes = reservedBytes;
            this.onFinalRelease = onFinalRelease;
        }

        private void retain() {
            for (; ; ) {
                int current = references.get();
                if (current <= 0) {
                    throw new IllegalStateException("request memory lease is already released");
                }
                if (current == Integer.MAX_VALUE) {
                    throw new IllegalStateException("request memory lease reference count overflow");
                }
                if (references.compareAndSet(current, current + 1)) {
                    return;
                }
            }
        }

        private void release() {
            int remaining = references.decrementAndGet();
            if (remaining < 0) {
                throw new IllegalStateException("request memory lease reference count underflow");
            }
            if (remaining == 0) {
                onFinalRelease.accept(reservedBytes);
            }
        }
    }
}
