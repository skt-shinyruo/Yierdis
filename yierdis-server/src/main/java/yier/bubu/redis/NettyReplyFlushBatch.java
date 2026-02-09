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
    private final IdentityHashMap<Channel, ChannelHandlerContext> targets = new IdentityHashMap<>();

    void record(ChannelHandlerContext ctx) {
        if (ctx == null) {
            return;
        }
        Channel ch = ctx.channel();
        if (ch == null) {
            return;
        }
        targets.put(ch, ctx);
    }

    void flushAll() {
        for (ChannelHandlerContext ctx : targets.values()) {
            safeFlush(ctx);
        }
        targets.clear();
    }

    private static void safeFlush(ChannelHandlerContext ctx) {
        if (ctx == null) {
            return;
        }
        try {
            ctx.flush();
        } catch (Throwable ignored) {
            // ignore
        }
    }
}

