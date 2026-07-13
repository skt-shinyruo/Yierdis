package yier.bubu.redis.storage.memory.internal.hash;

import yier.bubu.redis.memory.api.NativeCapacityExceededException;

public final class HashCapacityPolicy {
    public static final int MIN_CAPACITY = 16;
    public static final int MAX_CAPACITY = 1 << 30;

    private HashCapacityPolicy() {
    }

    public static Decision nextAction(int capacity, int size, int filledSlots, int tombstones) {
        validate(capacity, size, filledSlots, tombstones);

        if (filledSlots > capacity - capacity / 4) {
            if (capacity == MAX_CAPACITY) {
                throw new NativeCapacityExceededException("hash table capacity limit reached: " + capacity);
            }
            return new Decision(Action.GROW, capacity << 1);
        }
        if (tombstones > Math.max(size, capacity / 8)) {
            return new Decision(Action.COMPACT, capacity);
        }
        if (capacity > MIN_CAPACITY && size < capacity / 8) {
            return new Decision(Action.SHRINK, capacity >>> 1);
        }
        return new Decision(Action.NONE, capacity);
    }

    private static void validate(int capacity, int size, int filledSlots, int tombstones) {
        if (capacity < MIN_CAPACITY || capacity > MAX_CAPACITY || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("capacity must be a power of two in [" + MIN_CAPACITY + ", "
                    + MAX_CAPACITY + "]: " + capacity);
        }
        if (size < 0 || filledSlots < size || filledSlots > capacity || tombstones < 0
                || filledSlots - size != tombstones) {
            throw new IllegalArgumentException("invalid hash table metrics");
        }
    }

    public enum Action {
        NONE,
        GROW,
        COMPACT,
        SHRINK
    }

    public record Decision(Action action, int targetCapacity) {
        public Decision {
            if (action == null) {
                throw new NullPointerException("action");
            }
            if (targetCapacity < MIN_CAPACITY || targetCapacity > MAX_CAPACITY
                    || (targetCapacity & (targetCapacity - 1)) != 0) {
                throw new IllegalArgumentException("invalid target capacity: " + targetCapacity);
            }
        }
    }
}
