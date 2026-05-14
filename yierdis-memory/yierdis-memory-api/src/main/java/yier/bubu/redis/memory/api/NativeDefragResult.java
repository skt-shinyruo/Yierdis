package yier.bubu.redis.memory.api;

public record NativeDefragResult(
        boolean moved,
        boolean skippedPinned,
        boolean skippedBudget,
        long movedBytes
) {
    public static NativeDefragResult moved(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be >= 0");
        }
        return new NativeDefragResult(true, false, false, bytes);
    }

    public static NativeDefragResult skippedPinnedObject() {
        return new NativeDefragResult(false, true, false, 0L);
    }

    public static NativeDefragResult skippedMoveBudget() {
        return new NativeDefragResult(false, false, true, 0L);
    }
}
