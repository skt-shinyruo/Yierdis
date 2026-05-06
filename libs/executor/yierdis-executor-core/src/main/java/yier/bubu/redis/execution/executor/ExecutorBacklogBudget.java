package yier.bubu.redis.execution.executor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executor backlog global budget:\n
 * - hard cap by task count (capacity)\n
 * - optional cap by retained bytes\n
 * <p>
 * Also exposes global backpressure hysteresis (high/low watermarks) derived from capacity/maxBytes.
 */
public final class ExecutorBacklogBudget {
    private final int queueCapacity;
    private final long queueMaxBytes;

    private final AtomicInteger queuedTasks = new AtomicInteger(0);
    private final AtomicLong queuedBytes = new AtomicLong(0);

    private final int globalBackpressureHighWatermark;
    private final int globalBackpressureLowWatermark;
    private final long globalBackpressureBytesHighWatermark;
    private final long globalBackpressureBytesLowWatermark;

    public ExecutorBacklogBudget(int queueCapacity, long queueMaxBytes) {
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be > 0");
        }
        if (queueMaxBytes < 0) {
            throw new IllegalArgumentException("queueMaxBytes must be >= 0");
        }
        this.queueCapacity = queueCapacity;
        this.queueMaxBytes = queueMaxBytes;

        int globalHigh = defaultGlobalBackpressureHighWatermark(queueCapacity);
        int globalLow = defaultGlobalBackpressureLowWatermark(globalHigh);
        long globalBytesHigh = defaultGlobalBackpressureBytesHighWatermark(queueMaxBytes);
        long globalBytesLow = defaultGlobalBackpressureBytesLowWatermark(globalBytesHigh);

        this.globalBackpressureHighWatermark = globalHigh;
        this.globalBackpressureLowWatermark = globalLow;
        this.globalBackpressureBytesHighWatermark = globalBytesHigh;
        this.globalBackpressureBytesLowWatermark = globalBytesLow;
    }

    public int queueCapacity() {
        return queueCapacity;
    }

    public long queueMaxBytes() {
        return queueMaxBytes;
    }

    public int queuedTasks() {
        return queuedTasks.get();
    }

    public long queuedBytes() {
        return queuedBytes.get();
    }

    public int globalBackpressureHighWatermark() {
        return globalBackpressureHighWatermark;
    }

    public int globalBackpressureLowWatermark() {
        return globalBackpressureLowWatermark;
    }

    public long globalBackpressureBytesHighWatermark() {
        return globalBackpressureBytesHighWatermark;
    }

    public long globalBackpressureBytesLowWatermark() {
        return globalBackpressureBytesLowWatermark;
    }

    public boolean isGlobalBackpressureHigh() {
        boolean tasksHigh = queuedTasks.get() >= globalBackpressureHighWatermark;
        boolean bytesHigh = globalBackpressureBytesHighWatermark > 0 && queuedBytes.get() >= globalBackpressureBytesHighWatermark;
        return tasksHigh || bytesHigh;
    }

    public boolean isGlobalBackpressureCleared() {
        boolean tasksOk = queuedTasks.get() <= globalBackpressureLowWatermark;
        boolean bytesOk = globalBackpressureBytesHighWatermark <= 0 || queuedBytes.get() <= globalBackpressureBytesLowWatermark;
        return tasksOk && bytesOk;
    }

    public boolean tryReserveSlot() {
        for (; ; ) {
            int cur = queuedTasks.get();
            if (cur >= queueCapacity) {
                return false;
            }
            if (queuedTasks.compareAndSet(cur, cur + 1)) {
                return true;
            }
        }
    }

    public void releaseSlot() {
        int now = queuedTasks.decrementAndGet();
        if (now < 0) {
            // Best-effort: avoid underflow breaking future reservations.
            queuedTasks.set(0);
        }
    }

    public boolean tryReserveQueuedBytes(int bytes) {
        if (queueMaxBytes <= 0 || bytes <= 0) {
            return true;
        }
        for (; ; ) {
            long cur = queuedBytes.get();
            long next = cur + bytes;
            if (next < 0) {
                // overflow guard: treat as OOM / reject.
                return false;
            }
            if (next > queueMaxBytes) {
                return false;
            }
            if (queuedBytes.compareAndSet(cur, next)) {
                return true;
            }
        }
    }

    public void releaseQueuedBytes(int bytes) {
        if (queueMaxBytes <= 0 || bytes <= 0) {
            return;
        }
        long now = queuedBytes.addAndGet(-bytes);
        if (now < 0) {
            // Best-effort: avoid underflow breaking future reservations.
            queuedBytes.set(0);
        }
    }

    private static int defaultGlobalBackpressureHighWatermark(int queueCapacity) {
        if (queueCapacity <= 0) {
            return 1;
        }
        int high = (queueCapacity * 3 + 3) / 4; // ceil(0.75 * cap)
        if (high <= 0) {
            high = 1;
        }
        if (high > queueCapacity) {
            high = queueCapacity;
        }
        return high;
    }

    private static int defaultGlobalBackpressureLowWatermark(int globalHigh) {
        if (globalHigh <= 1) {
            return 0;
        }
        int low = globalHigh / 2;
        if (low >= globalHigh) {
            low = globalHigh - 1;
        }
        return Math.max(0, low);
    }

    private static long defaultGlobalBackpressureBytesHighWatermark(long queueMaxBytes) {
        if (queueMaxBytes <= 0) {
            return 0;
        }
        long high = (queueMaxBytes * 3) / 4;
        if (high <= 0) {
            return queueMaxBytes;
        }
        if (high > queueMaxBytes) {
            return queueMaxBytes;
        }
        return high;
    }

    private static long defaultGlobalBackpressureBytesLowWatermark(long globalBytesHigh) {
        if (globalBytesHigh <= 0) {
            return 0;
        }
        long low = globalBytesHigh / 2;
        if (low >= globalBytesHigh) {
            low = globalBytesHigh - 1;
        }
        return Math.max(0, low);
    }
}

