package yier.bubu.redis.execution.api;

public record ConnectionStatsView(
        int pending,
        long pendingBytes,
        boolean inputDisabledByExecutor,
        boolean inputPausedByReply,
        boolean closing,
        long commandsEnqueued,
        long commandsExecuted,
        long commandsRejected,
        long commandsSkippedClosing,
        long closeAfterReply,
        long backpressureEnter,
        long backpressureExit
) {
}
