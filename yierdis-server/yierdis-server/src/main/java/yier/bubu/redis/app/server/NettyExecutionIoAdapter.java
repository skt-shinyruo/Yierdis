package yier.bubu.redis.app.server;

import io.netty.channel.Channel;
import yier.bubu.redis.execution.executor.ExecutionConnection;
import yier.bubu.redis.execution.executor.ExecutionIoAdapter;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;

import java.util.Objects;

final class NettyExecutionIoAdapter implements ExecutionIoAdapter {
    @Override
    public boolean isActive(ExecutionConnection connection) {
        return connection instanceof NettyExecutionConnection netty && netty.channel().isActive();
    }

    @Override
    public boolean isWritable(ExecutionConnection connection) {
        return connection instanceof NettyExecutionConnection netty && netty.channel().isWritable();
    }

    @Override
    public void disableInput(ExecutionConnection connection) {
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
    public void enableInput(ExecutionConnection connection) {
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
    public void onClose(ExecutionConnection connection, Runnable callback) {
        if (callback == null || !(connection instanceof NettyExecutionConnection netty)) {
            return;
        }
        netty.channel().closeFuture().addListener(ignored -> callback.run());
    }

    @Override
    public void closeConnection(ExecutionConnection connection) {
        withChannel(connection, Channel::close);
    }

    private static void withChannel(ExecutionConnection connection, java.util.function.Consumer<Channel> action) {
        if (action == null || !(connection instanceof NettyExecutionConnection netty)) {
            return;
        }
        Channel channel = netty.channel();
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
