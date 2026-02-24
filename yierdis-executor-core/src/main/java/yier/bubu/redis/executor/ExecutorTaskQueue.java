package yier.bubu.redis.executor;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    public T poll() {
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            return globalQueue.poll();
        }
        return pollFairTask();
    }

    public boolean hasPendingTasks() {
        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
            return globalQueue != null && !globalQueue.isEmpty();
        }
        return !activeKeys.isEmpty();
    }

    public void drainLeftoverTasks(Consumer<T> recycler) {
        Objects.requireNonNull(recycler, "recycler");

        if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
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
    }

    private T pollFairTask() {
        for (; ; ) {
            K key = activeKeys.poll();
            if (key == null) {
                return null;
            }

            ExecutorKeyState<T> state = stateProvider.getOrCreate(key);
            T task = state.queue().poll();
            if (task == null) {
                // The key was scheduled but its queue is empty (may happen due to races). Unschedule it.
                state.scheduled().set(false);
                if (!state.queue().isEmpty() && state.scheduled().compareAndSet(false, true)) {
                    activeKeys.offer(key);
                }
                continue;
            }

            if (!state.queue().isEmpty()) {
                // More work for this key: re-queue it for round-robin fairness.
                activeKeys.offer(key);
            } else {
                // Try to unschedule; handle the race where a new task arrives while we're draining.
                state.scheduled().set(false);
                if (!state.queue().isEmpty() && state.scheduled().compareAndSet(false, true)) {
                    activeKeys.offer(key);
                }
            }

            return task;
        }
    }
}

