package yier.bubu.redis.runtime.embedded;

/**
 * CommitStream 的公开运行状态。
 */
public enum CommitStreamState {
    DISABLED,
    RUNNING,
    DRAINING,
    FAILED,
    CLOSED
}
