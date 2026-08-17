package yier.bubu.redis.execution.executor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 在一把私有锁下维护 GLOBAL FIFO 与 FAIR identity-key round-robin 状态。
 * backlog reservation 和执行语义由上层组件负责。
 */
final class ExecutorTaskQueue<K, T> {
    private final SchedulingPolicy schedulingPolicy;
    private final Object lock = new Object();
    private final ArrayDeque<T> globalQueue = new ArrayDeque<>();
    private final IdentityHashMap<K, FairState<T>> fairStates = new IdentityHashMap<>();
    private final ArrayDeque<K> activeKeys = new ArrayDeque<>();
    private T globalBlockedHead;
    private boolean globalBlockedHeadReady;
    private int blockedFairTasks;

    ExecutorTaskQueue(SchedulingPolicy schedulingPolicy) {
        this.schedulingPolicy = schedulingPolicy == null ? SchedulingPolicy.FAIR : schedulingPolicy;
    }

    void offer(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        synchronized (lock) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                globalQueue.addLast(task);
                return;
            }
            FairState<T> state = fairStateLocked(key);
            state.queue.addLast(task);
            scheduleFairStateLocked(key, state);
        }
    }

    boolean remove(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        synchronized (lock) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                return globalQueue.remove(task);
            }
            FairState<T> state = fairStates.get(key);
            if (state == null || !state.queue.remove(task)) {
                return false;
            }
            if (state.queue.isEmpty() && state.blockedHead == null && state.scheduled) {
                removeActiveKeyLocked(key);
                state.scheduled = false;
            }
            removeFairStateIfEmptyLocked(key, state);
            return true;
        }
    }

    T poll() {
        synchronized (lock) {
            return schedulingPolicy == SchedulingPolicy.GLOBAL ? pollGlobalLocked() : pollFairLocked();
        }
    }

    boolean hasRunnableTasks() {
        synchronized (lock) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                return globalBlockedHead == null ? !globalQueue.isEmpty() : globalBlockedHeadReady;
            }
            return !activeKeys.isEmpty();
        }
    }

    int deferredFairHeads() {
        synchronized (lock) {
            return blockedFairTasks;
        }
    }

    int deferredGlobalHeads() {
        synchronized (lock) {
            return globalBlockedHead == null ? 0 : 1;
        }
    }

    boolean block(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        synchronized (lock) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                if (globalBlockedHead != null) {
                    return false;
                }
                globalBlockedHead = task;
                globalBlockedHeadReady = false;
                return true;
            }
            FairState<T> state = fairStateLocked(key);
            if (state.blockedHead != null) {
                return false;
            }
            state.blockedHead = task;
            state.blockedHeadReady = false;
            blockedFairTasks++;
            if (state.scheduled) {
                removeActiveKeyLocked(key);
                state.scheduled = false;
            }
            return true;
        }
    }

    boolean retryAtHead(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        synchronized (lock) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                if (globalBlockedHead != null) {
                    return false;
                }
                globalBlockedHead = task;
                globalBlockedHeadReady = true;
                return true;
            }
            FairState<T> state = fairStateLocked(key);
            if (state.blockedHead != null) {
                return false;
            }
            state.blockedHead = task;
            state.blockedHeadReady = true;
            blockedFairTasks++;
            scheduleFairStateLocked(key, state);
            return true;
        }
    }

    boolean resumeBlocked(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        synchronized (lock) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                if (globalBlockedHead != task || globalBlockedHeadReady) {
                    return false;
                }
                globalBlockedHeadReady = true;
                return true;
            }
            FairState<T> state = fairStates.get(key);
            if (state == null || state.blockedHead != task || state.blockedHeadReady) {
                return false;
            }
            state.blockedHeadReady = true;
            scheduleFairStateLocked(key, state);
            return true;
        }
    }

    boolean cancelBlocked(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        synchronized (lock) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                if (globalBlockedHead != task) {
                    return false;
                }
                globalBlockedHead = null;
                globalBlockedHeadReady = false;
                return true;
            }
            FairState<T> state = fairStates.get(key);
            if (state == null || state.blockedHead != task) {
                return false;
            }
            state.blockedHead = null;
            state.blockedHeadReady = false;
            decrementBlockedFairTasksLocked();
            scheduleFairStateLocked(key, state);
            removeFairStateIfEmptyLocked(key, state);
            return true;
        }
    }

    void drainLeftoverTasks(Consumer<T> recycler) {
        Objects.requireNonNull(recycler, "recycler");
        List<T> leftovers = new ArrayList<>();
        synchronized (lock) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                if (globalBlockedHead != null) {
                    leftovers.add(globalBlockedHead);
                    globalBlockedHead = null;
                }
                globalBlockedHeadReady = false;
                leftovers.addAll(globalQueue);
                globalQueue.clear();
            } else {
                for (FairState<T> state : fairStates.values()) {
                    if (state.blockedHead != null) {
                        leftovers.add(state.blockedHead);
                    }
                    leftovers.addAll(state.queue);
                }
                fairStates.clear();
                activeKeys.clear();
                blockedFairTasks = 0;
            }
        }
        // 先解除队列所有权，再在锁外回收，避免关闭路径反向进入调度锁。
        Throwable failure = null;
        for (T leftover : leftovers) {
            try {
                recycler.accept(leftover);
            } catch (Throwable current) {
                if (failure == null) {
                    failure = current;
                } else if (failure != current) {
                    failure.addSuppressed(current);
                }
            }
        }
        rethrowRecycleFailure(failure);
    }

    int fairStateCount() {
        synchronized (lock) {
            return fairStates.size();
        }
    }

    private T pollGlobalLocked() {
        if (globalBlockedHead == null) {
            return globalQueue.pollFirst();
        }
        if (!globalBlockedHeadReady) {
            return null;
        }
        T task = globalBlockedHead;
        globalBlockedHead = null;
        globalBlockedHeadReady = false;
        return task;
    }

    private T pollFairLocked() {
        for (; ; ) {
            K key = activeKeys.pollFirst();
            if (key == null) {
                return null;
            }
            FairState<T> state = fairStates.get(key);
            if (state == null) {
                continue;
            }
            state.scheduled = false;

            if (state.blockedHead != null) {
                if (!state.blockedHeadReady) {
                    continue;
                }
                T task = state.blockedHead;
                state.blockedHead = null;
                state.blockedHeadReady = false;
                decrementBlockedFairTasksLocked();
                scheduleFairStateLocked(key, state);
                removeFairStateIfEmptyLocked(key, state);
                return task;
            }

            T task = state.queue.pollFirst();
            if (task == null) {
                removeFairStateIfEmptyLocked(key, state);
                continue;
            }
            scheduleFairStateLocked(key, state);
            removeFairStateIfEmptyLocked(key, state);
            return task;
        }
    }

    private FairState<T> fairStateLocked(K key) {
        FairState<T> state = fairStates.get(key);
        if (state == null) {
            state = new FairState<>();
            fairStates.put(key, state);
        }
        return state;
    }

    private void scheduleFairStateLocked(K key, FairState<T> state) {
        if (state.scheduled || (state.blockedHead != null && !state.blockedHeadReady)) {
            return;
        }
        if (state.blockedHead == null && state.queue.isEmpty()) {
            return;
        }
        state.scheduled = true;
        activeKeys.addLast(key);
    }

    private void removeFairStateIfEmptyLocked(K key, FairState<T> state) {
        if (!state.scheduled && state.blockedHead == null && state.queue.isEmpty()) {
            fairStates.remove(key);
        }
    }

    private void removeActiveKeyLocked(K key) {
        var iterator = activeKeys.iterator();
        while (iterator.hasNext()) {
            if (iterator.next() == key) {
                iterator.remove();
                return;
            }
        }
    }

    private void decrementBlockedFairTasksLocked() {
        if (blockedFairTasks <= 0) {
            throw new IllegalStateException("blocked FAIR task count underflow");
        }
        blockedFairTasks--;
    }

    private static void rethrowRecycleFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("executor task recycler failed", failure);
        }
    }

    private static final class FairState<T> {
        private final ArrayDeque<T> queue = new ArrayDeque<>();
        private T blockedHead;
        private boolean blockedHeadReady;
        private boolean scheduled;
    }
}
