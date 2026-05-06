package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.AttributeKey;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.executor.ExecutionIoAdapter;

import java.util.Objects;

final class NettyExecutionIoAdapter implements ExecutionIoAdapter<NettyExecutionConnection> {
    private static final AttributeKey<ByteBuf> PENDING_REPLY_BUFFER =
            AttributeKey.valueOf("yierdis.pendingReplyBuffer");

    @Override
    public boolean isActive(NettyExecutionConnection connection) {
        return connection != null && connection.channel().isActive();
    }

    @Override
    public boolean isWritable(NettyExecutionConnection connection) {
        return connection != null && connection.channel().isWritable();
    }

    @Override
    public void disableInput(NettyExecutionConnection connection) {
        withChannel(connection, channel -> channel.eventLoop().execute(() -> safeSetAutoRead(channel, false)));
    }

    @Override
    public void enableInput(NettyExecutionConnection connection) {
        withChannel(connection, channel -> channel.eventLoop().execute(() -> safeSetAutoRead(channel, true)));
    }

    @Override
    public void onClose(NettyExecutionConnection connection, Runnable callback) {
        if (connection == null || callback == null) {
            return;
        }
        connection.channel().closeFuture().addListener(ignored -> callback.run());
    }

    @Override
    public BytesSink newReplySink(NettyExecutionConnection connection) {
        Objects.requireNonNull(connection, "connection");
        Channel channel = connection.channel();
        ByteBuf out = channel.alloc().buffer();
        ByteBuf previous = channel.attr(PENDING_REPLY_BUFFER).getAndSet(out);
        if (previous != null) {
            previous.release();
        }
        return new NettyByteBufSink(out);
    }

    @Override
    public void writeBufferedReply(NettyExecutionConnection connection, boolean closeAfterReply) {
        Objects.requireNonNull(connection, "connection");
        Channel channel = connection.channel();
        ByteBuf out = channel.attr(PENDING_REPLY_BUFFER).getAndSet(null);
        if (out == null) {
            return;
        }
        if (closeAfterReply) {
            channel.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            return;
        }
        channel.write(out, channel.voidPromise());
    }

    @Override
    public void flushPending(Iterable<NettyExecutionConnection> touchedConnections) {
        if (touchedConnections == null) {
            return;
        }
        NettyReplyFlushBatch batch = new NettyReplyFlushBatch();
        for (NettyExecutionConnection connection : touchedConnections) {
            if (connection == null) {
                continue;
            }
            batch.record(connection.channel());
        }
        batch.flushAll();
    }

    private static void withChannel(NettyExecutionConnection connection, java.util.function.Consumer<Channel> action) {
        if (connection == null || action == null) {
            return;
        }
        Channel channel = connection.channel();
        if (channel == null) {
            return;
        }
        action.accept(channel);
    }

    private static void safeSetAutoRead(Channel channel, boolean enabled) {
        if (channel == null) {
            return;
        }
        try {
            channel.config().setAutoRead(enabled);
        } catch (Throwable ignored) {
            // ignore
        }
    }
}
