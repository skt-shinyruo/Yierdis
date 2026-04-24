package yier.bubu.redis;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.IdentityHashMap;

/**
 * Per-drain-tick flush coalescer.
 * <p>
 * Records channels that performed {@code write(...)} during the tick and flushes each channel at most once at the
 * end of the tick.
 */
final class NettyReplyFlushBatch {
    private final IdentityHashMap<Channel, Boolean> targets = new IdentityHashMap<>();

    void record(ChannelHandlerContext ctx) {
        if (ctx == null) {
            return;
        }
        record(ctx.channel());
    }

    void record(Channel channel) {
        if (channel == null) {
            return;
        }
        targets.put(channel, Boolean.TRUE);
    }

    void flushAll() {
        for (Channel channel : targets.keySet()) {
            safeFlush(channel);
        }
        targets.clear();
    }

    private static void safeFlush(Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.flush();
        } catch (Throwable ignored) {
            // ignore
        }
    }
}
