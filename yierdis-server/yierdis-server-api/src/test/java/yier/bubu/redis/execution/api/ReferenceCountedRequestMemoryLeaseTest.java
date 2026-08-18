package yier.bubu.redis.execution.api;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class ReferenceCountedRequestMemoryLeaseTest {
    @Test
    public void retainedReferencesReleaseBudgetExactlyOnceAtZero() {
        AtomicLong released = new AtomicLong();
        RequestMemoryLease first = new ReferenceCountedRequestMemoryLease(123, released::addAndGet);
        RequestMemoryLease second = first.retain();

        first.close();
        first.close();

        Assert.assertEquals(0L, released.get());

        second.close();

        Assert.assertEquals(123L, released.get());
    }

    @Test
    public void retainAfterFinalReleaseFails() {
        RequestMemoryLease lease = new ReferenceCountedRequestMemoryLease(1, ignored -> { });
        lease.close();

        Assert.assertThrows(IllegalStateException.class, lease::retain);
    }

    @Test
    public void negativeReservationIsRejected() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ReferenceCountedRequestMemoryLease(-1, ignored -> { }));
    }
}
