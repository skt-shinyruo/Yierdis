package yier.bubu.redis.app.server;

import java.util.concurrent.atomic.LongAdder;

final class ReplyEgressStats {
    private final LongAdder activeChunks = new LongAdder();
    private final LongAdder activeSources = new LongAdder();
    private final LongAdder oversizedReplies = new LongAdder();
    private final LongAdder cancelledSlots = new LongAdder();
    private final LongAdder failedSlots = new LongAdder();
    private final LongAdder writeFailures = new LongAdder();
    private final LongAdder resultUnknownCloses = new LongAdder();
    private final LongAdder shutdownTimeouts = new LongAdder();

    void chunkAdded() {
        activeChunks.increment();
    }

    void chunkReleased() {
        activeChunks.decrement();
    }

    void sourceAdded() {
        activeSources.increment();
    }

    void sourceReleased() {
        activeSources.decrement();
    }

    void oversizedReply() {
        oversizedReplies.increment();
    }

    void cancelledSlot() {
        cancelledSlots.increment();
    }

    void failedSlot() {
        failedSlots.increment();
    }

    void writeFailure() {
        writeFailures.increment();
    }

    void resultUnknownClose() {
        resultUnknownCloses.increment();
    }

    void shutdownTimeout() {
        shutdownTimeouts.increment();
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
