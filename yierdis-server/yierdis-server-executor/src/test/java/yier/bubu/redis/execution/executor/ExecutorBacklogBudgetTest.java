package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CapacityRegistration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    @Test
    public void combinedReservationIsAllOrNone() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(2, 8);

        Assert.assertNull(budget.tryReserve(6));
        Assert.assertEquals(ExecutorAdmissionAttempt.BlockReason.QUEUE_BYTES, budget.tryReserve(3));
        Assert.assertEquals(1, budget.queuedTasks());
        Assert.assertEquals(6L, budget.queuedBytes());

        budget.release(6);
        Assert.assertEquals(0, budget.queuedTasks());
        Assert.assertEquals(0L, budget.queuedBytes());
    }

    @Test
    public void waiterCallbackCanReenterBudgetAfterCapacityIsReleased() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 8);
        Assert.assertNull(budget.tryReserve(8));
        AtomicReference<ExecutorAdmissionAttempt.BlockReason> reentrantResult = new AtomicReference<>();
        budget.onCapacityAvailable(8, () -> reentrantResult.set(budget.tryReserve(8)));

        budget.release(8);

        Assert.assertNull(reentrantResult.get());
        Assert.assertEquals(1, budget.queuedTasks());
        Assert.assertEquals(8L, budget.queuedBytes());
        budget.release(8);
    }

    @Test
    public void waiterCallbackRunsWithoutHoldingTheBudgetLock() throws Exception {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 8);
        Assert.assertNull(budget.tryReserve(8));
        AtomicReference<Boolean> probeCompletedInsideCallback = new AtomicReference<>(false);
        CountDownLatch probeCompleted = new CountDownLatch(1);
        budget.onCapacityAvailable(8, () -> {
            Thread.ofPlatform().start(() -> {
                budget.tryReserve(8);
                probeCompleted.countDown();
            });
            try {
                probeCompletedInsideCallback.set(probeCompleted.await(1, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });

        budget.release(8);

        Assert.assertTrue("capacity callback must run after releasing the budget lock",
                probeCompletedInsideCallback.get());
        budget.release(8);
    }

    @Test
    public void shutdownWakesEveryWaiterOnce() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 0);
        Assert.assertNull(budget.tryReserve(0));
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        budget.onCapacityAvailable(0, first::incrementAndGet);
        budget.onCapacityAvailable(0, second::incrementAndGet);

        budget.wakeAllCapacityWaiters();
        budget.wakeAllCapacityWaiters();

        Assert.assertEquals(1, first.get());
        Assert.assertEquals(1, second.get());
        budget.release(0);
    }
}
