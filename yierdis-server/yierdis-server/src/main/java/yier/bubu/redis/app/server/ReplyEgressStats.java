package yier.bubu.redis.app.server;

import java.util.concurrent.atomic.LongAdder;

final class ReplyEgressStats {
    private static final ReplyEgressStats NOOP = new ReplyEgressStats(false);

    private final boolean enabled;
    private final LongAdder activeChunks = new LongAdder();
    private final LongAdder activeSources = new LongAdder();
    private final LongAdder oversizedReplies = new LongAdder();
    private final LongAdder cancelledSlots = new LongAdder();
    private final LongAdder failedSlots = new LongAdder();
    private final LongAdder writeFailures = new LongAdder();
    private final LongAdder resultUnknownCloses = new LongAdder();
    private final LongAdder shutdownTimeouts = new LongAdder();

    ReplyEgressStats() {
        this(true);
    }

    private ReplyEgressStats(boolean enabled) {
        this.enabled = enabled;
    }

    static ReplyEgressStats noop() {
        return NOOP;
    }

    void chunkAdded() {
        if (enabled) {
            activeChunks.increment();
        }
    }

    void chunkReleased() {
        if (enabled) {
            activeChunks.decrement();
        }
    }

    void sourceAdded() {
        if (enabled) {
            activeSources.increment();
        }
    }

    void sourceReleased() {
        if (enabled) {
            activeSources.decrement();
        }
    }

    void oversizedReply() {
        if (enabled) {
            oversizedReplies.increment();
        }
    }

    void cancelledSlot() {
        if (enabled) {
            cancelledSlots.increment();
        }
    }

    void failedSlot() {
        if (enabled) {
            failedSlots.increment();
        }
    }

    void writeFailure() {
        if (enabled) {
            writeFailures.increment();
        }
    }

    void resultUnknownClose() {
        if (enabled) {
            resultUnknownCloses.increment();
        }
    }

    void shutdownTimeout() {
        if (enabled) {
            shutdownTimeouts.increment();
        }
    }

    Snapshot snapshot() {
        return new Snapshot(
                activeChunks.sum(),
                activeSources.sum(),
                oversizedReplies.sum(),
                cancelledSlots.sum(),
                failedSlots.sum(),
                writeFailures.sum(),
                resultUnknownCloses.sum(),
                shutdownTimeouts.sum()
        );
    }

    record Snapshot(
            long activeChunks,
            long activeSources,
            long oversizedReplies,
            long cancelledSlots,
            long failedSlots,
            long writeFailures,
            long resultUnknownCloses,
            long shutdownTimeouts
    ) {
    }
}
