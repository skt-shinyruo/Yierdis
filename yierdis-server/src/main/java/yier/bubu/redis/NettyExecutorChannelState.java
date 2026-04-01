package yier.bubu.redis;

// 执行器的 per-channel 状态容器（server 私有）。用于承载公平调度队列与调度标志，避免把调度细节放入协议层的 ConnectionContext。

import yier.bubu.redis.executor.ExecutorKeyState;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

final class NettyExecutorChannelState implements ExecutorKeyState<NettyExecutorTask> {
    private final ConcurrentLinkedQueue<NettyExecutorTask> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean scheduled = new AtomicBoolean(false);

    @Override
    public ConcurrentLinkedQueue<NettyExecutorTask> queue() {
        return queue;
    }

    @Override
    public AtomicBoolean scheduled() {
        return scheduled;
    }
}
