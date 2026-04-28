package yier.bubu.redis.contract;

/**
 * Read-only view of executor/transport connection counters.
 */
public interface ConnectionStatsView {
    int pending();

    long pendingBytes();

    boolean inputDisabledByExecutor();

    boolean closing();

    long commandsEnqueued();

    long commandsExecuted();

    long commandsRejected();

    long commandsSkippedClosing();

    long closeAfterReply();

    long backpressureEnter();

    long backpressureExit();
}
