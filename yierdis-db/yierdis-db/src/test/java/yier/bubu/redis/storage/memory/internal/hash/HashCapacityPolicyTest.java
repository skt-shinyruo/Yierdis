package yier.bubu.redis.storage.memory.internal.hash;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.storage.memory.internal.hash.HashCapacityPolicy.Action;

public class HashCapacityPolicyTest {
    @Test
    public void usesApprovedThresholdsAndPrecedence() {
        Assert.assertEquals(Action.NONE, next(16, 10, 12, 2).action());
        Assert.assertEquals(Action.GROW, next(16, 11, 13, 2).action());
        Assert.assertEquals(Action.COMPACT, next(64, 20, 41, 21).action());
        Assert.assertEquals(64, next(64, 20, 41, 21).targetCapacity());
        Assert.assertEquals(Action.SHRINK, next(64, 7, 7, 0).action());
        Assert.assertEquals(32, next(64, 7, 7, 0).targetCapacity());
        Assert.assertEquals(Action.NONE, next(16, 1, 1, 0).action());
        Assert.assertEquals("grow wins when it is eligible with compaction", Action.GROW,
                next(64, 24, 49, 25).action());
    }

    @Test
    public void rejectsInvalidTableMetrics() {
        assertThrows(IllegalArgumentException.class, () -> next(15, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> next(8, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> next(16, -1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> next(16, 1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> next(16, 1, 1, -1));
        assertThrows(IllegalArgumentException.class, () -> next(16, 17, 17, 0));
    }

    @Test
    public void refusesGrowthBeyondMaximumCapacity() {
        assertThrows(NativeCapacityExceededException.class,
                () -> next(HashCapacityPolicy.MAX_CAPACITY,
                        (HashCapacityPolicy.MAX_CAPACITY / 4) * 3,
                        (HashCapacityPolicy.MAX_CAPACITY / 4) * 3 + 1,
                        1));
    }

    private static HashCapacityPolicy.Decision next(int capacity, int size, int filledSlots, int tombstones) {
        return HashCapacityPolicy.nextAction(capacity, size, filledSlots, tombstones);
    }

    private static void assertThrows(Class<? extends Throwable> type, ThrowingRunnable action) {
        try {
            action.run();
            Assert.fail("expected " + type.getName());
        } catch (Throwable thrown) {
            if (!type.isInstance(thrown)) {
                throw new AssertionError("expected " + type.getName() + " but got " + thrown, thrown);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }
}
