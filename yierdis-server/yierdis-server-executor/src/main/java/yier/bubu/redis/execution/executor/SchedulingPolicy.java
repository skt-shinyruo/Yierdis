package yier.bubu.redis.execution.executor;

/**
 * Executor task scheduling policy.
 * <p>
 * GLOBAL: single global FIFO backlog.\n
 * FAIR: per-connection queues with round-robin scheduling across active connections.
 */
public enum SchedulingPolicy {
    GLOBAL,
    FAIR
}

