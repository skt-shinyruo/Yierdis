package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.CapacityRegistration;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * executor backlog 的任务数、保留字节和容量等待者共享同一状态锁。
 * 容量回调只在锁外执行，允许 ingress 在回调中重新尝试 reservation。
 */
public final class ExecutorBacklogBudget {
    private final int queueCapacity;
    private final long queueMaxBytes;
    private final Object lock = new Object();
    private final ArrayDeque<CapacityWaiter> capacityWaiters = new ArrayDeque<>();
    private int queuedTasks;
    private long queuedBytes;
    private boolean capacityWaitersClosed;

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

    public int queuedTasks() {
        synchronized (lock) {
            return queuedTasks;
        }
    }

    public long queuedBytes() {
        synchronized (lock) {
            return queuedBytes;
        }
    }

    public boolean isGlobalBackpressureHigh() {
        synchronized (lock) {
            boolean tasksHigh = queuedTasks >= globalBackpressureHighWatermark;
            boolean bytesHigh = globalBackpressureBytesHighWatermark > 0
                    && queuedBytes >= globalBackpressureBytesHighWatermark;
            return tasksHigh || bytesHigh;
        }
    }

    public boolean isGlobalBackpressureCleared() {
        synchronized (lock) {
            boolean tasksOk = queuedTasks <= globalBackpressureLowWatermark;
            boolean bytesOk = globalBackpressureBytesHighWatermark <= 0
                    || queuedBytes <= globalBackpressureBytesLowWatermark;
            return tasksOk && bytesOk;
        }
    }

    public boolean canEverReserveQueuedBytes(int bytes) {
        return queueMaxBytes <= 0 || Math.max(0, bytes) <= queueMaxBytes;
    }

    ExecutorAdmissionAttempt.BlockReason tryReserve(int retainedBytes) {
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
        synchronized (lock) {
            if (queuedTasks >= queueCapacity) {
                return ExecutorAdmissionAttempt.BlockReason.QUEUE_SLOTS;
            }
            if (queueMaxBytes > 0 && retainedBytes > queueMaxBytes - queuedBytes) {
                return ExecutorAdmissionAttempt.BlockReason.QUEUE_BYTES;
            }
            queuedTasks++;
            if (queueMaxBytes > 0) {
                queuedBytes += retainedBytes;
            }
            return null;
        }
    }

    void release(int retainedBytes) {
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
        List<Runnable> callbacks;
        synchronized (lock) {
            if (queuedTasks <= 0) {
                throw new IllegalStateException("executor backlog task reservation underflow");
            }
            if (queueMaxBytes > 0 && retainedBytes > queuedBytes) {
                throw new IllegalStateException("executor backlog byte reservation underflow");
            }
            queuedTasks--;
            if (queueMaxBytes > 0) {
                queuedBytes -= retainedBytes;
            }
            callbacks = detachEligibleWaitersLocked();
        }
        runCallbacks(callbacks);
    }

    CapacityRegistration onCapacityAvailable(int retainedBytes, Runnable callback) {
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
        CapacityWaiter waiter = new CapacityWaiter(retainedBytes, Objects.requireNonNull(callback, "callback"));
        List<Runnable> callbacks;
        synchronized (lock) {
            if (capacityWaitersClosed) {
                callbacks = List.of(waiter.detachLocked());
            } else {
                capacityWaiters.addLast(waiter);
                callbacks = detachEligibleWaitersLocked();
            }
        }
        runCallbacks(callbacks);
        return waiter;
    }

    void wakeAllCapacityWaiters() {
        List<Runnable> callbacks = new ArrayList<>();
        synchronized (lock) {
            capacityWaitersClosed = true;
            CapacityWaiter waiter;
            while ((waiter = capacityWaiters.pollFirst()) != null) {
                Runnable callback = waiter.detachLocked();
                if (callback != null) {
                    callbacks.add(callback);
                }
            }
        }
        runCallbacks(callbacks);
    }

    private List<Runnable> detachEligibleWaitersLocked() {
        if (queuedTasks >= queueCapacity || capacityWaiters.isEmpty()) {
            return List.of();
        }
        List<Runnable> callbacks = new ArrayList<>();
        var iterator = capacityWaiters.iterator();
        while (iterator.hasNext()) {
            CapacityWaiter waiter = iterator.next();
            if (!hasByteCapacityLocked(waiter.retainedBytes)) {
                continue;
            }
            iterator.remove();
            Runnable callback = waiter.detachLocked();
            if (callback != null) {
                callbacks.add(callback);
            }
        }
        return callbacks;
    }

    private boolean hasByteCapacityLocked(int retainedBytes) {
        if (queueMaxBytes <= 0 || retainedBytes <= 0) {
            return true;
        }
        return queuedBytes <= queueMaxBytes - retainedBytes;
    }

    private static void runCallbacks(List<Runnable> callbacks) {
        for (Runnable callback : callbacks) {
            try {
                callback.run();
            } catch (Throwable ignored) {
            }
        }
    }

    private final class CapacityWaiter implements CapacityRegistration {
        private final int retainedBytes;
        private Runnable callback;
        private boolean active = true;

        private CapacityWaiter(int retainedBytes, Runnable callback) {
            this.retainedBytes = retainedBytes;
            this.callback = Objects.requireNonNull(callback, "callback");
        }

        private Runnable detachLocked() {
            if (!active) {
                return null;
            }
            active = false;
            Runnable detached = callback;
            callback = null;
            return detached;
        }

        @Override
        public void cancel() {
            synchronized (lock) {
                if (!active) {
                    return;
                }
                active = false;
                callback = null;
                capacityWaiters.remove(this);
            }
        }
    }

    private static int defaultGlobalBackpressureHighWatermark(int queueCapacity) {
        if (queueCapacity <= 0) {
            return 1;
        }
        int high = queueCapacity - queueCapacity / 4;
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
        long high = (queueMaxBytes / 4) * 3 + ((queueMaxBytes % 4) * 3) / 4;
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
