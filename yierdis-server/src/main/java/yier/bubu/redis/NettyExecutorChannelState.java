package yier.bubu.redis;

// 执行器的 per-channel 状态容器（server 私有）。用于承载公平调度队列与调度标志，避免把调度细节放入协议层的 ConnectionContext。

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

final class NettyExecutorChannelState {
    private static final AttributeKey<NettyExecutorChannelState> KEY =
            AttributeKey.valueOf("yierdis.executorChannelState");

    static NettyExecutorChannelState getOrCreate(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        Attribute<NettyExecutorChannelState> attr = channel.attr(KEY);
        NettyExecutorChannelState existing = attr.get();
        if (existing != null) {
            return existing;
        }
        NettyExecutorChannelState created = new NettyExecutorChannelState();
        NettyExecutorChannelState raced = attr.setIfAbsent(created);
        return raced == null ? created : raced;
    }

    private final ConcurrentLinkedQueue<NettyExecutorTask> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean scheduled = new AtomicBoolean(false);

    ConcurrentLinkedQueue<NettyExecutorTask> queue() {
        return queue;
    }

    AtomicBoolean scheduled() {
        return scheduled;
    }
}
