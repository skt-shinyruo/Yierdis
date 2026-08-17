package yier.bubu.redis.execution.executor;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CapacityRegistration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ExecutorBacklogBudgetTest {
    @Test
    public void capacityWaiterWakesOnceAfterReservationIsReleased() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 32);
        Assert.assertNull(budget.tryReserve(8));

        AtomicInteger wakeups = new AtomicInteger();
        CapacityRegistration registration = budget.onCapacityAvailable(
                8,
                wakeups::incrementAndGet
        );
        Assert.assertEquals(0, wakeups.get());

        budget.release(8);
        Assert.assertEquals(1, wakeups.get());

        registration.cancel();
        Assert.assertEquals(1, wakeups.get());
    }

    @Test
    public void capacityWaiterCanBeCancelledWithoutRetainingCallback() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 0);
        Assert.assertNull(budget.tryReserve(0));

        AtomicInteger wakeups = new AtomicInteger();
        CapacityRegistration registration = budget.onCapacityAvailable(
                0,
                wakeups::incrementAndGet
        );
        registration.cancel();
        budget.release(0);

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

        Assert.assertNull(budget.tryReserve(2));
        Assert.assertEquals(ExecutorAdmissionAttempt.BlockReason.QUEUE_SLOTS, budget.tryReserve(0));
        Assert.assertEquals(2, budget.queuedTasks());
        Assert.assertEquals(8L, budget.queuedBytes());

        budget.release(2);
        budget.release(6);
        Assert.assertEquals(0, budget.queuedTasks());
        Assert.assertEquals(0L, budget.queuedBytes());
    }

    @Test
    public void releaseRejectsUnderflowWithoutChangingEitherCounter() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(2, 8);

        Assert.assertThrows(IllegalArgumentException.class, () -> budget.release(-1));
        Assert.assertThrows(IllegalStateException.class, () -> budget.release(0));
        Assert.assertEquals(0, budget.queuedTasks());
        Assert.assertEquals(0L, budget.queuedBytes());

        Assert.assertNull(budget.tryReserve(4));
        Assert.assertThrows(IllegalStateException.class, () -> budget.release(5));
        Assert.assertEquals(1, budget.queuedTasks());
        Assert.assertEquals(4L, budget.queuedBytes());

        budget.release(4);
        Assert.assertEquals(0, budget.queuedTasks());
        Assert.assertEquals(0L, budget.queuedBytes());
    }

    @Test
    public void waiterRegisteredAfterCapacityReturnsWakesImmediatelyOnce() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 8);
        AtomicInteger wakeups = new AtomicInteger();

        CapacityRegistration registration = budget.onCapacityAvailable(8, wakeups::incrementAndGet);

        Assert.assertEquals(1, wakeups.get());
        registration.cancel();
        Assert.assertEquals(1, wakeups.get());
    }

    @Test
    public void releaseWakesEveryEligibleWaiterWithoutHeadOfLineBlocking() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(2, 10);
        Assert.assertNull(budget.tryReserve(6));
        Assert.assertNull(budget.tryReserve(4));
        List<String> wakeups = new ArrayList<>();
        budget.onCapacityAvailable(5, () -> wakeups.add("large"));
        budget.onCapacityAvailable(4, () -> wakeups.add("exact"));
        budget.onCapacityAvailable(1, () -> wakeups.add("small"));

        budget.release(4);
        Assert.assertEquals(List.of("exact", "small"), wakeups);

        budget.release(6);
        Assert.assertEquals(List.of("exact", "small", "large"), wakeups);
    }

    @Test
    public void failingCapacityCallbackDoesNotBlockLaterCallbacks() {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 0);
        Assert.assertNull(budget.tryReserve(0));
        AtomicInteger wakeups = new AtomicInteger();
        budget.onCapacityAvailable(0, () -> {
            throw new IllegalStateException("boom");
        });
        budget.onCapacityAvailable(0, wakeups::incrementAndGet);

        budget.release(0);

        Assert.assertEquals(1, wakeups.get());
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
    public void concurrentReservationsCommitTaskAndBytesTogether() throws Exception {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger slotBlocked = new AtomicInteger();
        Runnable contender = () -> {
            await(start);
            ExecutorAdmissionAttempt.BlockReason blocked = budget.tryReserve(8);
            if (blocked == null) {
                accepted.incrementAndGet();
            } else if (blocked == ExecutorAdmissionAttempt.BlockReason.QUEUE_SLOTS) {
                slotBlocked.incrementAndGet();
            }
        };
        Thread first = Thread.ofPlatform().start(contender);
        Thread second = Thread.ofPlatform().start(contender);

        start.countDown();
        first.join();
        second.join();

        Assert.assertEquals(1, accepted.get());
        Assert.assertEquals(1, slotBlocked.get());
        Assert.assertEquals(1, budget.queuedTasks());
        Assert.assertEquals(8L, budget.queuedBytes());
        budget.release(8);
    }

    @Test
    public void claimedWaiterCannotBeCancelledWhileEarlierCallbackRuns() throws Exception {
        ExecutorBacklogBudget budget = new ExecutorBacklogBudget(1, 0);
        Assert.assertNull(budget.tryReserve(0));
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch allowFirstToReturn = new CountDownLatch(1);
        AtomicInteger secondWakeups = new AtomicInteger();
        budget.onCapacityAvailable(0, () -> {
            firstEntered.countDown();
            await(allowFirstToReturn);
        });
        CapacityRegistration second = budget.onCapacityAvailable(0, secondWakeups::incrementAndGet);
        Thread releaser = Thread.ofPlatform().start(() -> budget.release(0));

        boolean firstCallbackStarted = firstEntered.await(1, TimeUnit.SECONDS);
        if (!firstCallbackStarted) {
            allowFirstToReturn.countDown();
            releaser.join();
            Assert.fail("first capacity callback did not start");
        }
        Thread canceller = Thread.ofPlatform().start(second::cancel);
        canceller.join(1_000);
        boolean cancelCompletedWhileCallbackBlocked = !canceller.isAlive();
        allowFirstToReturn.countDown();
        canceller.join();
        releaser.join();

        Assert.assertTrue("cancel must not wait for a capacity callback", cancelCompletedWhileCallbackBlocked);
        Assert.assertEquals(1, secondWakeups.get());
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
        AtomicInteger registeredAfterShutdown = new AtomicInteger();
        CapacityRegistration late = budget.onCapacityAvailable(0, registeredAfterShutdown::incrementAndGet);
        Assert.assertEquals(1, registeredAfterShutdown.get());
        late.cancel();
        budget.release(0);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
