package yier.bubu.redis.execution.executor;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Executor-internal queue scheduling component.\n
 * <p>
 * Isolates {@link SchedulingPolicy#GLOBAL} vs {@link SchedulingPolicy#FAIR} branches and round-robin details.\n
 * This component is intentionally "semantics-free": it only queues/polls tasks. Budget/backpressure/execution
 * are handled by higher-level components.
 */
public final class ExecutorTaskQueue<K, T> {
    private final SchedulingPolicy schedulingPolicy;
    private final ArrayBlockingQueue<T> globalQueue;
    private final ExecutorKeyStateProvider<K, T> stateProvider;

    // FAIR scheduling uses per-key queues + round-robin scheduling.
    private final ConcurrentLinkedQueue<K> activeKeys = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<K> blockedKeys = new ConcurrentLinkedQueue<>();
    private final AtomicInteger blockedFairTasks = new AtomicInteger();
    private final AtomicReference<T> globalBlockedHead = new AtomicReference<>();
    private final AtomicBoolean globalBlockedHeadReady = new AtomicBoolean();

    public ExecutorTaskQueue(SchedulingPolicy schedulingPolicy,
                             ArrayBlockingQueue<T> globalQueue,
                             ExecutorKeyStateProvider<K, T> stateProvider) {
        this.schedulingPolicy = schedulingPolicy == null ? SchedulingPolicy.FAIR : schedulingPolicy;
        this.globalQueue = globalQueue;
        this.stateProvider = stateProvider;
        if (this.schedulingPolicy == SchedulingPolicy.GLOBAL && this.globalQueue == null) {
            throw new IllegalArgumentException("globalQueue must not be null when schedulingPolicy is GLOBAL");
        }
        if (this.schedulingPolicy == SchedulingPolicy.FAIR && this.stateProvider == null) {
            throw new IllegalArgumentException("stateProvider must not be null when schedulingPolicy is FAIR");
        }
    }

    public boolean offer(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");

        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            return globalQueue.offer(task);
        }

        ExecutorKeyState<T> state = stateProvider.getOrCreate(key);
        state.queue().offer(task);
        if (state.scheduled().compareAndSet(false, true)) {
            activeKeys.offer(key);
        }
        return true;
    }

    public boolean remove(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            return globalQueue.remove(task);
        }
        return stateProvider.getOrCreate(key).queue().remove(task);
    }

    public T poll() {
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            return pollGlobalTask();
        }
        return pollFairTask();
    }

    public boolean hasPendingTasks() {
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            return globalBlockedHead.get() != null || (globalQueue != null && !globalQueue.isEmpty());
        }
        return blockedFairTasks.get() > 0 || !activeKeys.isEmpty();
    }

    public boolean hasRunnableTasks() {
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            T blocked = globalBlockedHead.get();
            return blocked == null
                    ? globalQueue != null && !globalQueue.isEmpty()
                    : globalBlockedHeadReady.get();
        }
        return !activeKeys.isEmpty();
    }

    public int deferredFairHeads() {
        return blockedFairTasks.get();
    }

    public int deferredGlobalHeads() {
        return globalBlockedHead.get() == null ? 0 : 1;
    }

    public boolean block(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            if (!globalBlockedHead.compareAndSet(null, task)) {
                return false;
            }
            globalBlockedHeadReady.set(false);
            return true;
        }

        ExecutorKeyState<T> state = stateProvider.getOrCreate(key);
        if (!state.blockedHead().compareAndSet(null, task)) {
            return false;
        }
        state.blockedHeadReady().set(false);
        blockedKeys.offer(key);
        blockedFairTasks.incrementAndGet();
        return true;
    }

    public boolean resumeBlocked(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            if (globalBlockedHead.get() != task) {
                return false;
            }
            return globalBlockedHeadReady.compareAndSet(false, true);
        }

        ExecutorKeyState<T> state = stateProvider.getOrCreate(key);
        if (state.blockedHead().get() != task || !state.blockedHeadReady().compareAndSet(false, true)) {
            return false;
        }
        scheduleFairKey(key, state);
        return true;
    }

    public boolean cancelBlocked(K key, T task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            if (!globalBlockedHead.compareAndSet(task, null)) {
                return false;
            }
            globalBlockedHeadReady.set(false);
            return true;
        }

        ExecutorKeyState<T> state = stateProvider.getOrCreate(key);
        if (!state.blockedHead().compareAndSet(task, null)) {
            return false;
        }
        state.blockedHeadReady().set(false);
        decrementBlockedFairTasks();
        return true;
    }

    public void drainLeftoverTasks(Consumer<T> recycler) {
        Objects.requireNonNull(recycler, "recycler");

        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            T blocked = globalBlockedHead.getAndSet(null);
            globalBlockedHeadReady.set(false);
            if (blocked != null) {
                recycler.accept(blocked);
            }
            T t;
            while ((t = globalQueue.poll()) != null) {
                recycler.accept(t);
            }
            return;
        }

        K key;
        while ((key = activeKeys.poll()) != null) {
            ExecutorKeyState<T> state = stateProvider.getOrCreate(key);
            T t;
            while ((t = state.queue().poll()) != null) {
                recycler.accept(t);
            }
            state.scheduled().set(false);
        }

        while ((key = blockedKeys.poll()) != null) {
            ExecutorKeyState<T> state = stateProvider.getOrCreate(key);
            T blocked = state.blockedHead().getAndSet(null);
            state.blockedHeadReady().set(false);
            if (blocked != null) {
                decrementBlockedFairTasks();
                recycler.accept(blocked);
            }
        }
    }

    private T pollGlobalTask() {
        for (; ; ) {
            T blocked = globalBlockedHead.get();
            if (blocked == null) {
                return globalQueue.poll();
            }
            if (!globalBlockedHeadReady.get()) {
                return null;
            }
            if (globalBlockedHead.compareAndSet(blocked, null)) {
                globalBlockedHeadReady.set(false);
                return blocked;
            }
        }
    }

    private T pollFairTask() {
        for (; ; ) {
            K key = activeKeys.poll();
            if (key == null) {
                return null;
            }

            ExecutorKeyState<T> state = stateProvider.getOrCreate(key);
            T blocked = state.blockedHead().get();
            if (blocked != null) {
                if (!state.blockedHeadReady().get()) {
                    state.scheduled().set(false);
                    if (state.blockedHeadReady().get()) {
                        scheduleFairKey(key, state);
                    }
                    continue;
                }
                if (state.blockedHead().compareAndSet(blocked, null)) {
                    state.blockedHeadReady().set(false);
                    decrementBlockedFairTasks();
                    scheduleFairKeyAfterPoll(key, state);
                    return blocked;
                }
                continue;
            }

            T task = state.queue().poll();
            if (task == null) {
                // The key was scheduled but its queue is empty (may happen due to races). Unschedule it.
                state.scheduled().set(false);
                if (!state.queue().isEmpty() && state.scheduled().compareAndSet(false, true)) {
                    activeKeys.offer(key);
                }
                continue;
            }

            scheduleFairKeyAfterPoll(key, state);

            return task;
        }
    }

    private void scheduleFairKeyAfterPoll(K key, ExecutorKeyState<T> state) {
        if (!state.queue().isEmpty()) {
            activeKeys.offer(key);
            return;
        }
        state.scheduled().set(false);
        if ((!state.queue().isEmpty() || state.blockedHeadReady().get())
                && state.scheduled().compareAndSet(false, true)) {
            activeKeys.offer(key);
        }
    }

    private void scheduleFairKey(K key, ExecutorKeyState<T> state) {
        if (state.scheduled().compareAndSet(false, true)) {
            activeKeys.offer(key);
        }
    }

    private void decrementBlockedFairTasks() {
        blockedFairTasks.updateAndGet(current -> Math.max(0, current - 1));
    }
}
