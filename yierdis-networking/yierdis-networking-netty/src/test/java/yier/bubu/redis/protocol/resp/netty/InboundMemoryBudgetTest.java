package yier.bubu.redis.protocol.resp.netty;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void transferWaitsWhenTheRealTemporaryPeakCannotFit() {
        InboundMemoryBudget budget = new InboundMemoryBudget(160);
        InboundConnectionMemory connection = connection("a", 100, () -> { });

        Assert.assertEquals(InboundMemoryBudget.ReservationResult.RESERVED, budget.tryReserve(connection, 90));
        Assert.assertEquals(InboundMemoryBudget.ReservationResult.WAITING, budget.tryTransfer(connection, 80, 90));
        Assert.assertEquals(90L, budget.stats().reservedBytes());
        Assert.assertEquals(1, budget.stats().waitingConnections());
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

    private static InboundConnectionMemory connection(String id, long hardLimit, Runnable onResume) {
        return new InboundConnectionMemory(id, hardLimit, Runnable::run, onResume);
    }
}
