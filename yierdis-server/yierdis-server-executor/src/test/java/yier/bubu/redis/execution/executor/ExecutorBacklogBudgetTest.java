package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CapacityRegistration;

import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorBacklogBudgetTest {
    @Test
    public void capacityWaiterWakesOnceAfterSlotIsReleased() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 32);
        Assert.assertTrue(budget.tryReserveSlot());
        Assert.assertTrue(budget.tryReserveQueuedBytes(8));

        AtomicInteger wakeups = new AtomicInteger();
        CapacityRegistration registration = budget.onCapacityAvailable(
                8,
                wakeups::incrementAndGet
        );
        Assert.assertEquals(0, wakeups.get());

        budget.releaseQueuedBytes(8);
        Assert.assertEquals(0, wakeups.get());
        budget.releaseSlot();
        Assert.assertEquals(1, wakeups.get());

        registration.cancel();
        Assert.assertEquals(1, wakeups.get());
    }

    @Test
    public void capacityWaiterCanBeCancelledWithoutRetainingCallback() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 0);
        Assert.assertTrue(budget.tryReserveSlot());

        AtomicInteger wakeups = new AtomicInteger();
        CapacityRegistration registration = budget.onCapacityAvailable(
                0,
                wakeups::incrementAndGet
        );
        registration.cancel();
        budget.releaseSlot();

        Assert.assertEquals(0, wakeups.get());
    }

    @Test
    public void requestLargerThanByteBudgetCanNeverBeReserved() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(4, 32);
        Assert.assertFalse(budget.canEverReserveQueuedBytes(33));
        Assert.assertTrue(budget.canEverReserveQueuedBytes(32));
        Assert.assertTrue(new ExecutorBacklogBudget(4, 0).canEverReserveQueuedBytes(Integer.MAX_VALUE));
    }
}
