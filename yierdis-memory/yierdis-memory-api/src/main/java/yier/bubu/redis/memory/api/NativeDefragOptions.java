package yier.bubu.redis.memory.api;

public record NativeDefragOptions(
        long maxMoveBytes,
        long maxObjects,
        long timeBudgetNanos
) {
    public NativeDefragOptions {
        if (maxMoveBytes < 0) {
            throw new IllegalArgumentException("maxMoveBytes must be >= 0");
        }
        if (maxObjects < 0) {
            throw new IllegalArgumentException("maxObjects must be >= 0");
        }
        if (timeBudgetNanos < 0) {
            throw new IllegalArgumentException("timeBudgetNanos must be >= 0");
        }
    }
}
