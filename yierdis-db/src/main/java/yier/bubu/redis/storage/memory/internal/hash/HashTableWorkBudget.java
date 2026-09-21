package yier.bubu.redis.storage.memory.internal.hash;

public record HashTableWorkBudget(long maxInspectedSlots, long timeLimitNanos) {
    public HashTableWorkBudget {
        if (maxInspectedSlots < 0L) {
            throw new IllegalArgumentException("maxInspectedSlots must be >= 0");
        }
        if (timeLimitNanos < 0L) {
            throw new IllegalArgumentException("timeLimitNanos must be >= 0");
        }
    }

    public static HashTableWorkBudget of(long maxInspectedSlots, long timeLimitNanos) {
        return new HashTableWorkBudget(maxInspectedSlots, timeLimitNanos);
    }
}
