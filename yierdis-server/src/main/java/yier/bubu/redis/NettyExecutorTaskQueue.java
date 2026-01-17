package yier.bubu.redis;

import io.netty.channel.Channel;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * 执行器内部队列调度组件：隔离 GLOBAL vs FAIR 的分支与 round-robin 细节。
 * <p>
 * 该组件保持“无业务语义”：只负责排队与取出任务，不负责预算/背压/执行与回收。
 */
final class NettyExecutorTaskQueue {
    private final NettyCommandExecutor.SchedulingPolicy schedulingPolicy;
    private final ArrayBlockingQueue<NettyExecutorTask> globalQueue;

    // FAIR scheduling uses per-channel queues + round-robin scheduling.
    private final ConcurrentLinkedQueue<Channel> activeChannels = new ConcurrentLinkedQueue<>();

    NettyExecutorTaskQueue(NettyCommandExecutor.SchedulingPolicy schedulingPolicy, ArrayBlockingQueue<NettyExecutorTask> globalQueue) {
        this.schedulingPolicy = schedulingPolicy == null ? NettyCommandExecutor.SchedulingPolicy.FAIR : schedulingPolicy;
        this.globalQueue = globalQueue;
    }

    boolean offer(Channel channel, NettyExecutorTask task) {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(task, "task");

        if (schedulingPolicy == NettyCommandExecutor.SchedulingPolicy.GLOBAL) {
            return globalQueue.offer(task);
        }

        NettyExecutorChannelState state = NettyExecutorChannelState.getOrCreate(channel);
        state.queue().offer(task);
        if (state.scheduled().compareAndSet(false, true)) {
            activeChannels.offer(channel);
        }
        return true;
    }

    NettyExecutorTask poll() {
        if (schedulingPolicy == NettyCommandExecutor.SchedulingPolicy.GLOBAL) {
            return globalQueue.poll();
        }
        return pollFairTask();
    }

    boolean hasPendingTasks() {
        if (schedulingPolicy == NettyCommandExecutor.SchedulingPolicy.GLOBAL) {
            return globalQueue != null && !globalQueue.isEmpty();
        }
        return !activeChannels.isEmpty();
    }

    void drainLeftoverCommands(Consumer<NettyExecutorTask> recycler) {
        Objects.requireNonNull(recycler, "recycler");

        if (schedulingPolicy == NettyCommandExecutor.SchedulingPolicy.GLOBAL) {
            NettyExecutorTask t;
            while ((t = globalQueue.poll()) != null) {
                recycler.accept(t);
            }
            return;
        }

        Channel channel;
        while ((channel = activeChannels.poll()) != null) {
            NettyExecutorChannelState state = NettyExecutorChannelState.getOrCreate(channel);
            NettyExecutorTask t;
            while ((t = state.queue().poll()) != null) {
                recycler.accept(t);
            }
            state.scheduled().set(false);
        }
    }

    private NettyExecutorTask pollFairTask() {
        for (; ; ) {
            Channel ch = activeChannels.poll();
            if (ch == null) {
                return null;
            }

            NettyExecutorChannelState state = NettyExecutorChannelState.getOrCreate(ch);
            NettyExecutorTask task = state.queue().poll();
            if (task == null) {
                // The channel was scheduled but its queue is empty (may happen due to races). Unschedule it.
                state.scheduled().set(false);
                if (!state.queue().isEmpty() && state.scheduled().compareAndSet(false, true)) {
                    activeChannels.offer(ch);
                }
                continue;
            }

            if (!state.queue().isEmpty()) {
                // More work for this channel: re-queue it for round-robin fairness.
                activeChannels.offer(ch);
            } else {
                // Try to unschedule; handle the race where a new task arrives while we're draining.
                state.scheduled().set(false);
                if (!state.queue().isEmpty() && state.scheduled().compareAndSet(false, true)) {
                    activeChannels.offer(ch);
                }
            }

            return task;
        }
    }
}

