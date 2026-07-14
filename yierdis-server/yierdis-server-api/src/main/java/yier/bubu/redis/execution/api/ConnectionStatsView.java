package yier.bubu.redis.execution.api;

/**
 * Read-only view of executor/transport connection counters.
 */
public interface ConnectionStatsView {
    int pending();

    long pendingBytes();

    boolean inputDisabledByExecutor();

    default boolean inputPausedByReply() {
        return false;
    }

    boolean closing();

    long commandsEnqueued();

    long commandsExecuted();

    long commandsRejected();

    long commandsSkippedClosing();

    long closeAfterReply();

    long backpressureEnter();

    long backpressureExit();
}
