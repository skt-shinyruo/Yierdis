package yier.bubu.redis.protocol.resp.netty;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.RejectedExecutionException;

public class InboundMemoryBudgetTest {
    @Test
    public void releasesBackpressureInStrictFifoOrder() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        AtomicInteger resumedB = new AtomicInteger();
        AtomicInteger resumedC = new AtomicInteger();
        InboundConnectionMemory a = connection("a", 100, () -> { });
        InboundConnectionMemory b = connection("b", 100, resumedB::incrementAndGet);
        InboundConnectionMemory c = connection("c", 100, resumedC::incrementAndGet);

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(a, 75));
        Assert.assertTrue(budget.stats().backpressured());
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(b, 20));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(c, 20));

        budget.release(a, 25);

        Assert.assertEquals(1, resumedB.get());
        Assert.assertEquals(0, resumedC.get());
        Assert.assertEquals(1, budget.stats().waitingConnections());
        Assert.assertEquals(70L, budget.stats().reservedBytes());

        budget.release(a, 20);

        Assert.assertEquals(1, resumedC.get());
        Assert.assertEquals(0, budget.stats().waitingConnections());
        Assert.assertEquals(70L, budget.stats().reservedBytes());
    }

    @Test
    public void closesGrantedReservationBeforeItsEventLoopCallbackRuns() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        List<Runnable> pendingCallbacks = new ArrayList<>();
        InboundConnectionMemory a = connection("a", 100, () -> { });
        InboundConnectionMemory b = new InboundConnectionMemory("b", 100, pendingCallbacks::add, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(a, 75));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(b, 20));

        budget.release(a, 25);
        Assert.assertEquals(1, pendingCallbacks.size());

        b.close();
        budget.release(a, 50);

        Assert.assertEquals(0L, budget.stats().reservedBytes());
        pendingCallbacks.get(0).run();
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void requestThatCannotEverFitIsRejectedRatherThanQueued() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        InboundConnectionMemory connection = connection("a", 100, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.REQUEST_LIMIT, budget.tryReserve(connection, 101));
        Assert.assertEquals(0, budget.stats().waitingConnections());
        Assert.assertEquals(1L, budget.stats().rejectedConnections());
    }

    @Test
    public void transferAdmitsTemporaryCopyPeakBeforeInputRelease() {
        InboundMemoryBudget budget = new InboundMemoryBudget(200);
        InboundConnectionMemory connection = connection("a", 100, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(connection, 90));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryTransfer(connection, 80, 90));
        Assert.assertEquals(170L, budget.stats().reservedBytes());
        Assert.assertEquals(170L, budget.stats().peakReservedBytes());

        budget.release(connection, 90);

        Assert.assertEquals(80L, budget.stats().reservedBytes());
        Assert.assertEquals(80L, connection.reservedBytes());
    }

    @Test
    public void transferThatCannotFitItsOwnCopyPeakIsRejectedRatherThanQueued() {
        InboundMemoryBudget budget = new InboundMemoryBudget(160);
        InboundConnectionMemory connection = connection("a", 100, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(connection, 90));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.REQUEST_LIMIT,
                budget.tryTransfer(connection, 80, 90));
        Assert.assertEquals(90L, budget.stats().reservedBytes());
        Assert.assertEquals(0, budget.stats().waitingConnections());
    }

    @Test
    public void transferWaitsOnlyForCapacityAnotherConnectionCanRelease() {
        InboundMemoryBudget budget = new InboundMemoryBudget(180);
        AtomicInteger resumed = new AtomicInteger();
        InboundConnectionMemory source = connection("source", 100, resumed::incrementAndGet);
        InboundConnectionMemory blocker = connection("blocker", 100, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(source, 90));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 30));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING,
                budget.tryTransfer(source, 80, 90));

        budget.release(blocker, 20);

        Assert.assertEquals(1, resumed.get());
        Assert.assertEquals(0, budget.stats().waitingConnections());
        Assert.assertEquals(180L, budget.stats().reservedBytes());

        budget.release(source, 90);
        budget.release(source, 80);
        budget.release(blocker, 10);
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void currentRequestProgressBypassesOnlyTheSoftHighWatermark() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        InboundConnectionMemory current = connection("current", 200, () -> { });
        InboundConnectionMemory fresh = connection("fresh", 100, () -> { });
        InboundConnectionMemory queued = connection("queued", 100, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(current, 75));
        Assert.assertTrue(budget.stats().backpressured());
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING,
                budget.tryReserveReadCredit(fresh, 10));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(queued, 10));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED,
                budget.tryReserveProgressReadCredit(current, 10));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING,
                budget.tryReserveProgressReadCredit(current, 16));

        budget.cancelWaiter(current);
        budget.cancelWaiter(fresh);
        budget.cancelWaiter(queued);
        budget.release(current, 85);
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void progressWaiterCanAdvanceAheadOfNewWorkWhileBackpressured() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        AtomicInteger progressWakeups = new AtomicInteger();
        AtomicInteger freshWakeups = new AtomicInteger();
        InboundConnectionMemory current = connection("current", 100, progressWakeups::incrementAndGet);
        InboundConnectionMemory holder = connection("holder", 100, () -> { });
        InboundConnectionMemory fresh = connection("fresh", 100, freshWakeups::incrementAndGet);

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(current, 20));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(holder, 80));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(fresh, 10));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING,
                budget.tryReserveProgressReadCredit(current, 10));

        budget.release(holder, 10);

        Assert.assertEquals(1, progressWakeups.get());
        Assert.assertEquals(0, freshWakeups.get());
        Assert.assertTrue(current.claimGrantedReservation(10));
        budget.cancelWaiter(fresh);
        budget.release(current, 30);
        budget.release(holder, 70);
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void closingBudgetCancelsWaitersButAllowsLateLeaseRelease() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        InboundConnectionMemory active = connection("active", 100, () -> { });
        InboundConnectionMemory waiting = connection("waiting", 100, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(active, 75));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(waiting, 20));

        budget.close();

        Assert.assertTrue(budget.stats().closed());
        Assert.assertEquals(0, budget.stats().waitingConnections());
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.CLOSED, budget.tryReserve(waiting, 1));
        Assert.assertEquals(75L, budget.stats().reservedBytes());

        budget.release(active, 75);

        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void closedConnectionAccountIsRetainedOnlyUntilItsLastDetachedLeaseReleases() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        InboundConnectionMemory connection = connection("detached", 100, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(connection, 60));
        Assert.assertEquals(1, budget.attachedAccountCountForTests());

        connection.close();
        Assert.assertEquals("the account must remain addressable while a detached lease can release it",
                1,
                budget.attachedAccountCountForTests());

        budget.release(connection, 60);

        Assert.assertEquals(0L, budget.stats().reservedBytes());
        Assert.assertEquals(0, budget.attachedAccountCountForTests());
    }

    @Test
    public void rejectsInvalidCapacitiesAmountsCreditsAndForeignAccounts() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new InboundMemoryBudget(-1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new InboundConnectionMemory("negative", -1, Runnable::run, () -> { }));
        Assert.assertThrows(NullPointerException.class,
                () -> new InboundConnectionMemory(null, 1, Runnable::run, () -> { }));
        Assert.assertThrows(NullPointerException.class,
                () -> new InboundConnectionMemory("missing-executor", 1, null, () -> { }));
        Assert.assertThrows(NullPointerException.class,
                () -> new InboundConnectionMemory("missing-callback", 1, Runnable::run, null));

        InboundMemoryBudget budget = new InboundMemoryBudget(10);
        InboundConnectionMemory connection = connection("invalid", 10, () -> { });
        Assert.assertThrows(NullPointerException.class, () -> budget.tryReserve(null, 1));
        Assert.assertThrows(IllegalArgumentException.class, () -> budget.tryReserve(connection, -1));
        Assert.assertThrows(IllegalArgumentException.class, () -> budget.tryTransfer(connection, -1, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> budget.tryTransfer(connection, 1, -1));
        Assert.assertThrows(IllegalArgumentException.class, () -> budget.tryTransfer(connection, 1, 1));
        Assert.assertThrows(IllegalArgumentException.class, () -> budget.release(connection, -1));
        Assert.assertThrows(NullPointerException.class, () -> budget.release((InboundConnectionMemory) null, 0));
        Assert.assertThrows(NullPointerException.class, () -> budget.cancelWaiter(null));
        Assert.assertThrows(IllegalStateException.class,
                () -> new InboundMemoryBudget(10).release(connection, 0));

        InboundMemoryBudget other = new InboundMemoryBudget(10);
        Assert.assertThrows(IllegalStateException.class, () -> other.tryReserve(connection, 0));
    }

    @Test
    public void repeatedWaiterIsNotDuplicatedAndCancellationLetsTheNextWaiterRun() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        AtomicInteger resumedB = new AtomicInteger();
        AtomicInteger resumedC = new AtomicInteger();
        InboundConnectionMemory blocker = connection("blocker", 100, () -> { });
        InboundConnectionMemory b = connection("b", 100, resumedB::incrementAndGet);
        InboundConnectionMemory c = connection("c", 100, resumedC::incrementAndGet);

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 75));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(b, 20));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(b, 10));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(c, 20));
        Assert.assertEquals(2, budget.stats().waitingConnections());

        budget.cancelWaiter(b);
        budget.release(blocker, 25);

        Assert.assertEquals(0, resumedB.get());
        Assert.assertEquals(1, resumedC.get());
        Assert.assertEquals(0, budget.stats().waitingConnections());
        budget.release(blocker, 50);
        budget.release(c, 20);
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void closedQueuedAccountIsSkippedWhenAnotherWaiterCanBeGranted() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        AtomicInteger resumed = new AtomicInteger();
        InboundConnectionMemory blocker = connection("blocker", 100, () -> { });
        InboundConnectionMemory closed = connection("closed", 100, () -> { });
        InboundConnectionMemory live = connection("live", 100, resumed::incrementAndGet);

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 75));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(closed, 20));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(live, 20));
        closed.account().markClosed();

        budget.release(blocker, 25);

        Assert.assertEquals(1, resumed.get());
        Assert.assertEquals(0, budget.stats().waitingConnections());
        budget.release(blocker, 50);
        budget.release(live, 20);
    }

    @Test
    public void rejectedResumeExecutorClosesConnectionAndReturnsGrantedBytes() {
        InboundMemoryBudget budget = new InboundMemoryBudget(100);
        InboundConnectionMemory blocker = connection("blocker", 100, () -> { });
        InboundConnectionMemory rejected = new InboundConnectionMemory(
                "rejected",
                100,
                command -> { throw new RejectedExecutionException("rejected"); },
                () -> { }
        );

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(blocker, 75));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryReserve(rejected, 20));

        budget.release(blocker, 25);

        Assert.assertTrue(rejected.closed());
        Assert.assertEquals(50L, budget.stats().reservedBytes());
        budget.release(blocker, 50);
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void countersSaturateRejectUnderflowAndZeroCapacityHasDefinedBehavior() {
        InboundMemoryBudget budget = new InboundMemoryBudget(0);
        InboundConnectionMemory connection = connection("zero", 0, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(connection, 0));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.REQUEST_LIMIT, budget.tryReserve(connection, 1));
        Assert.assertFalse(budget.stats().backpressured());

        budget.adjustReadCredit(Long.MAX_VALUE);
        budget.adjustReadCredit(1);
        budget.adjustRetainedInputCapacity(3);
        budget.adjustConsolidation(4);
        Assert.assertEquals(Long.MAX_VALUE, budget.stats().readCreditBytes());
        Assert.assertThrows(IllegalStateException.class, () -> budget.adjustReadCredit(Long.MIN_VALUE));
        Assert.assertThrows(IllegalStateException.class, () -> budget.adjustRetainedInputCapacity(-4));
        Assert.assertThrows(IllegalStateException.class, () -> budget.adjustConsolidation(-5));
        budget.adjustRetainedInputCapacity(-3);
        budget.adjustConsolidation(-4);

        Assert.assertEquals(Long.MAX_VALUE, InboundMemoryBudget.saturatedAdd(Long.MAX_VALUE, 1));
        Assert.assertEquals(Long.MAX_VALUE, InboundMemoryBudget.saturatedAdd(-1, 1));
        budget.close();
        budget.close();
        Assert.assertTrue(budget.stats().closed());
    }

    private static InboundConnectionMemory connection(String id, long hardLimit, Runnable onResume) {
        return new InboundConnectionMemory(id, hardLimit, Runnable::run, onResume);
    }
}
