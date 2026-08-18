package yier.bubu.redis.app.server;

import io.netty.util.concurrent.EventExecutor;
import yier.bubu.redis.execution.executor.SerialOwnerExecutor;

import java.util.Objects;

final class NettySerialOwnerExecutor implements SerialOwnerExecutor {
    private final EventExecutor delegate;

    NettySerialOwnerExecutor(EventExecutor delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(Objects.requireNonNull(command, "command"));
    }

    @Override
    public boolean inOwnerThread() {
        return delegate.inEventLoop();
    }
}
