package yier.bubu.redis.execution.executor;

import java.util.concurrent.Executor;

public interface SerialOwnerExecutor extends Executor {
    boolean inOwnerThread();

    default void requireOwnerThread() {
        if (!inOwnerThread()) {
            throw new IllegalStateException("not on the serial owner thread");
        }
    }
}
