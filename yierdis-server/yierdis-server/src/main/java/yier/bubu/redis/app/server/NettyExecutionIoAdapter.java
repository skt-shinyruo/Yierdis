package yier.bubu.redis.app.server;

import io.netty.channel.Channel;
import yier.bubu.redis.execution.executor.ExecutionIoAdapter;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;

import java.util.Objects;

final class NettyExecutionIoAdapter implements ExecutionIoAdapter<NettyExecutionConnection> {
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
        withChannel(connection, channel -> channel.eventLoop().execute(() -> {
            InboundReadCreditHandler readCredits = channel.pipeline().get(InboundReadCreditHandler.class);
            if (readCredits != null) {
                readCredits.pauseExecutorInput();
                return;
            }
            safeSetAutoRead(channel, false);
        }));
    }

    @Override
    public void enableInput(NettyExecutionConnection connection) {
        withChannel(connection, channel -> channel.eventLoop().execute(() -> {
            InboundReadCreditHandler readCredits = channel.pipeline().get(InboundReadCreditHandler.class);
            if (readCredits != null) {
                readCredits.resumeExecutorInput();
                return;
            }
            safeSetAutoRead(channel, true);
        }));
    }

    @Override
    public void onClose(NettyExecutionConnection connection, Runnable callback) {
        if (connection == null || callback == null) {
            return;
        }
        connection.channel().closeFuture().addListener(ignored -> callback.run());
    }

    @Override
    public void closeConnection(NettyExecutionConnection connection) {
        withChannel(connection, Channel::close);
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
