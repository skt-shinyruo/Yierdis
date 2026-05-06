package yier.bubu.redis.execution.executor;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-key scheduling state for {@link ExecutorTaskQueue} when using {@link SchedulingPolicy#FAIR}.
 * <p>
 * Implementations are responsible for storage/lifecycle (e.g. Netty Channel.attr).
 */
public interface ExecutorKeyState<T> {
    Queue<T> queue();

    AtomicBoolean scheduled();
}

