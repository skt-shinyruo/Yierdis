package yier.bubu.redis.app.server;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class OutboundMemoryBudgetTest {
    @Test
    public void enforcesGlobalConnectionAndSingleLimitsBeforeAllocation() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(1024L);
        OutboundConnectionMemory a = budget.openConnection(600L);
        OutboundConnectionMemory b = budget.openConnection(600L);
        OutboundConnectionMemory c = budget.openConnection(600L);
        OutboundMemoryLease a1 = a.reserve(200L, 400L).orElseThrow();
        OutboundMemoryLease a2 = a.reserve(200L, 400L).orElseThrow();
        Assert.assertTrue(a.reserve(300L, 400L).isEmpty());
        OutboundMemoryLease b1 = b.reserve(300L, 400L).orElseThrow();
        Assert.assertTrue(c.reserve(500L, 600L).isEmpty());

        a1.close();
        a1.close();
        Assert.assertEquals(500L, budget.stats().reservedBytes());
        a2.close();
        b1.close();
        Assert.assertEquals(0L, budget.stats().reservedBytes());
        Assert.assertEquals(0, budget.stats().activeSlots());
    }

    @Test
    public void tracksConvertedAllocationSeparatelyFromAdmittedReservation() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(1024L);
        OutboundConnectionMemory connection = budget.openConnection(800L);
        OutboundMemoryLease lease = connection.reserve(400L, 600L).orElseThrow();

        Assert.assertTrue(lease.convertToAllocated(128L));
        Assert.assertEquals(400L, budget.stats().reservedBytes());
        Assert.assertEquals(128L, budget.stats().allocatedBytes());
        Assert.assertEquals(400L, budget.stats().peakReservedBytes());
        Assert.assertEquals(128L, budget.stats().peakAllocatedBytes());

        lease.releaseAllocated(128L);
        Assert.assertEquals(400L, budget.stats().reservedBytes());
        Assert.assertEquals(0L, budget.stats().allocatedBytes());
        lease.close();
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void saturatingProjectionRejectsOverflowWithoutChangingCounters() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(Long.MAX_VALUE);
        OutboundConnectionMemory connection = budget.openConnection(Long.MAX_VALUE);
        OutboundMemoryLease full = connection.reserve(Long.MAX_VALUE, Long.MAX_VALUE).orElseThrow();

        Assert.assertTrue(connection.reserve(1L, Long.MAX_VALUE).isEmpty());
        Assert.assertEquals(Long.MAX_VALUE, budget.stats().reservedBytes());
        full.close();
        Assert.assertEquals(0L, budget.stats().reservedBytes());
    }

    @Test
    public void closeCancelsWaitersButKeepsActiveLeasesValidUntilTheirFinalClose() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(400L);
        OutboundConnectionMemory first = budget.openConnection(400L);
        OutboundConnectionMemory waiting = budget.openConnection(400L);
        OutboundMemoryLease lease = first.reserve(400L, 400L).orElseThrow();
        AtomicInteger wakeups = new AtomicInteger();

        Assert.assertTrue(waiting.awaitCapacity(100L, 400L, wakeups::incrementAndGet));
        waiting.close();
        budget.close();
        Assert.assertTrue(budget.stats().closed());
        Assert.assertEquals(400L, budget.stats().reservedBytes());
        Assert.assertEquals(1, budget.stats().activeConnections());
        Assert.assertEquals(1L, budget.stats().activeSlots());
        Assert.assertTrue(first.reserve(1L, 400L).isEmpty());
        Assert.assertTrue(lease.convertToAllocated(100L));

        OutboundConnectionMemory afterClose = budget.openConnection(400L);
        Assert.assertTrue(afterClose.reserve(1L, 400L).isEmpty());
        Assert.assertFalse(afterClose.awaitCapacity(1L, 400L, () -> Assert.fail("closed budget must not wake waiters")));

        lease.close();
        Assert.assertEquals(0, wakeups.get());
        Assert.assertEquals(0L, budget.stats().reservedBytes());
        Assert.assertEquals(0L, budget.stats().allocatedBytes());
        Assert.assertEquals(0, budget.stats().activeConnections());
    }

    @Test
    public void wakesOnlyTheOldestLiveWaiterWhenCapacityReturns() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(400L);
        OutboundConnectionMemory holder = budget.openConnection(400L);
        OutboundConnectionMemory first = budget.openConnection(400L);
        OutboundConnectionMemory second = budget.openConnection(400L);
        OutboundMemoryLease lease = holder.reserve(400L, 400L).orElseThrow();
        AtomicInteger firstWakeups = new AtomicInteger();
        AtomicInteger secondWakeups = new AtomicInteger();

        Assert.assertTrue(first.awaitCapacity(100L, 400L, firstWakeups::incrementAndGet));
        Assert.assertTrue(second.awaitCapacity(100L, 400L, secondWakeups::incrementAndGet));
        lease.close();

        Assert.assertEquals(1, firstWakeups.get());
        Assert.assertEquals(0, secondWakeups.get());
        first.close();
        Assert.assertEquals(1, secondWakeups.get());
    }

    @Test
    public void grantedRetryCannotBeDisplacedBeforeItConsumesCapacity() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(400L);
        OutboundConnectionMemory holder = budget.openConnection(400L);
        OutboundConnectionMemory waiting = budget.openConnection(400L);
        OutboundConnectionMemory competing = budget.openConnection(400L);
        OutboundMemoryLease pressure = holder.reserve(400L, 400L).orElseThrow();
        AtomicInteger wakeups = new AtomicInteger();
        Optional<OutboundMemoryLease> displaced = Optional.empty();
        OutboundMemoryLease admitted = null;
        try {
            Assert.assertTrue(waiting.awaitCapacity(100L, 400L, wakeups::incrementAndGet));

            pressure.close();
            Assert.assertEquals(1, wakeups.get());

            displaced = competing.reserve(400L, 400L);
            Assert.assertTrue("a signalled retry must own the advisory grant", displaced.isEmpty());

            admitted = waiting.reserve(100L, 400L).orElseThrow();
            Assert.assertEquals(100L, budget.stats().reservedBytes());
            Assert.assertEquals(0, budget.stats().waitingConnections());
        } finally {
            if (admitted != null) {
                admitted.close();
            }
            displaced.ifPresent(OutboundMemoryLease::close);
            pressure.close();
            holder.close();
            waiting.close();
            competing.close();
            budget.close();
        }
    }

    @Test
    public void failedGrantCallbackHandsCapacityToTheNextWaiter() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(400L);
        OutboundConnectionMemory holder = budget.openConnection(400L);
        OutboundConnectionMemory failed = budget.openConnection(400L);
        OutboundConnectionMemory next = budget.openConnection(400L);
        OutboundMemoryLease pressure = holder.reserve(400L, 400L).orElseThrow();
        AtomicInteger nextWakeups = new AtomicInteger();
        OutboundMemoryLease admitted = null;
        try {
            Assert.assertTrue(failed.awaitCapacity(100L, 400L, () -> {
                throw new IllegalStateException("injected callback failure");
            }));
            Assert.assertTrue(next.awaitCapacity(100L, 400L, nextWakeups::incrementAndGet));

            pressure.close();

            Assert.assertEquals(1, nextWakeups.get());
            admitted = next.reserve(100L, 400L).orElseThrow();
            Assert.assertEquals(0, budget.stats().waitingConnections());
        } finally {
            if (admitted != null) {
                admitted.close();
            }
            pressure.close();
            holder.close();
            failed.close();
            next.close();
            budget.close();
        }
    }

    @Test
    public void wakesAnExpandedLeaseWaiterWithoutReplacingItsControlReservation() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(400L);
        OutboundConnectionMemory waitingConnection = budget.openConnection(400L);
        OutboundConnectionMemory holderConnection = budget.openConnection(400L);
        OutboundMemoryLease waitingLease = waitingConnection.reserve(100L, 400L).orElseThrow();
        OutboundMemoryLease holderLease = holderConnection.reserve(300L, 400L).orElseThrow();
        AtomicInteger wakeups = new AtomicInteger();
        try {
            Assert.assertFalse(waitingLease.tryReserveAdditional(200L, 400L));
            Assert.assertTrue(waitingLease.awaitAdditionalCapacity(200L, 400L, wakeups::incrementAndGet));

            holderLease.close();

            Assert.assertEquals(1, wakeups.get());
            Assert.assertTrue(waitingLease.tryReserveAdditional(200L, 400L));
            Assert.assertEquals(300L, waitingLease.reservedBytes());
            Assert.assertEquals(300L, budget.stats().reservedBytes());
        } finally {
            holderLease.close();
            waitingLease.close();
        }
    }

    @Test
    public void grantedExpansionCannotBeDisplacedBeforeTheLeaseConsumesIt() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(400L);
        OutboundConnectionMemory waitingConnection = budget.openConnection(400L);
        OutboundConnectionMemory holderConnection = budget.openConnection(400L);
        OutboundConnectionMemory competingConnection = budget.openConnection(400L);
        OutboundMemoryLease waitingLease = waitingConnection.reserve(100L, 400L).orElseThrow();
        OutboundMemoryLease holderLease = holderConnection.reserve(300L, 400L).orElseThrow();
        AtomicInteger wakeups = new AtomicInteger();
        Optional<OutboundMemoryLease> displaced = Optional.empty();
        try {
            Assert.assertTrue(waitingLease.awaitAdditionalCapacity(200L, 400L, wakeups::incrementAndGet));

            holderLease.close();
            Assert.assertEquals(1, wakeups.get());

            displaced = competingConnection.reserve(300L, 400L);
            Assert.assertTrue("a signalled expansion must own the advisory grant", displaced.isEmpty());
            Assert.assertTrue(waitingLease.tryReserveAdditional(200L, 400L));
            Assert.assertEquals(300L, waitingLease.reservedBytes());
        } finally {
            displaced.ifPresent(OutboundMemoryLease::close);
            holderLease.close();
            waitingLease.close();
            holderConnection.close();
            waitingConnection.close();
            competingConnection.close();
            budget.close();
        }
    }

    @Test
    public void connectionLimitedExpansionDoesNotBlockAnIndependentReservation() {
        OutboundMemoryBudget budget = new OutboundMemoryBudget(1_000L);
        OutboundConnectionMemory constrained = budget.openConnection(600L);
        OutboundConnectionMemory independent = budget.openConnection(600L);
        OutboundMemoryLease retainedCapacity = constrained.reserve(500L, 600L).orElseThrow();
        OutboundMemoryLease reply = constrained.reserve(100L, 600L).orElseThrow();
        AtomicInteger wakeups = new AtomicInteger();
        try {
            Assert.assertFalse(reply.tryReserveAdditional(100L, 600L));
            Assert.assertTrue(reply.awaitAdditionalCapacity(100L, 600L, wakeups::incrementAndGet));

            OutboundMemoryLease unrelated = independent.reserve(100L, 600L).orElseThrow();
            unrelated.close();

            retainedCapacity.close();
            Assert.assertEquals(1, wakeups.get());
            Assert.assertTrue(reply.tryReserveAdditional(100L, 600L));
        } finally {
            retainedCapacity.close();
            reply.close();
            independent.close();
            constrained.close();
        }
    }

    @Test
    public void rejectsInvalidReservationAndConnectionLimits() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new OutboundMemoryBudget(0L));
        OutboundMemoryBudget budget = new OutboundMemoryBudget(100L);
        Assert.assertThrows(IllegalArgumentException.class, () -> budget.openConnection(101L));
        OutboundConnectionMemory connection = budget.openConnection(100L);
        Assert.assertThrows(IllegalArgumentException.class, () -> connection.reserve(0L, 100L));
        Assert.assertThrows(IllegalArgumentException.class, () -> connection.reserve(-1L, 100L));
        Assert.assertThrows(IllegalArgumentException.class, () -> connection.reserve(1L, 0L));
        Assert.assertThrows(IllegalArgumentException.class, () -> connection.reserve(1L, -1L));
    }
}
