package yier.bubu.redis.memory.api;

public record NativeDefragReport(
        long scannedObjects,
        long movedObjects,
        long movedBytes,
        long skippedPinnedObjects,
        long skippedBudgetObjects,
        long failedMoves,
        boolean stoppedByByteBudget,
        boolean stoppedByObjectBudget,
        boolean stoppedByTimeBudget
) {
    public NativeDefragReport {
        if (scannedObjects < 0) {
            throw new IllegalArgumentException("scannedObjects must be >= 0");
        }
        if (movedObjects < 0) {
            throw new IllegalArgumentException("movedObjects must be >= 0");
        }
        if (movedBytes < 0) {
            throw new IllegalArgumentException("movedBytes must be >= 0");
        }
        if (skippedPinnedObjects < 0) {
            throw new IllegalArgumentException("skippedPinnedObjects must be >= 0");
        }
        if (skippedBudgetObjects < 0) {
            throw new IllegalArgumentException("skippedBudgetObjects must be >= 0");
        }
        if (failedMoves < 0) {
            throw new IllegalArgumentException("failedMoves must be >= 0");
        }
    }
}
