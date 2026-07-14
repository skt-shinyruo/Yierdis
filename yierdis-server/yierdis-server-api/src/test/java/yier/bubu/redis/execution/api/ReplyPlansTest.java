package yier.bubu.redis.execution.api;

import org.junit.Assert;
import org.junit.Test;

public class ReplyPlansTest {
    @Test
    public void bulkStringIncludesExactResp2FramingAtDecimalBoundaries() {
        Assert.assertEquals(5L, ReplyPlans.bulkString(-1, 0L).encodedUpperBoundBytes());
        Assert.assertEquals(6L, ReplyPlans.bulkString(0, 0L).encodedUpperBoundBytes());
        Assert.assertEquals(15L, ReplyPlans.bulkString(9, 0L).encodedUpperBoundBytes());
        Assert.assertEquals(17L, ReplyPlans.bulkString(10, 0L).encodedUpperBoundBytes());
        Assert.assertEquals(106L, ReplyPlans.bulkString(99, 0L).encodedUpperBoundBytes());
        Assert.assertEquals(108L, ReplyPlans.bulkString(100, 0L).encodedUpperBoundBytes());
    }

    @Test
    public void arrayAddsItsCompleteHeaderAndRetainedSourceCharge() {
        ReplyPlan plan = ReplyPlans.bulkStringArray(10, 123L, 456L);

        Assert.assertEquals(128L, plan.encodedUpperBoundBytes());
        Assert.assertEquals(456L, plan.retainedSourceBytes());
        Assert.assertFalse(plan.reserveMaximum());
    }

    @Test
    public void arithmeticSaturatesAndMaximumUsesNoExactPayloadClaim() {
        ReplyPlan saturated = ReplyPlans.raw(Long.MAX_VALUE, 1L);
        Assert.assertEquals(Long.MAX_VALUE, saturated.totalUpperBoundBytes());

        ReplyPlan maximum = ReplyPlan.maximum();
        Assert.assertTrue(maximum.reserveMaximum());
        Assert.assertEquals(0L, maximum.encodedUpperBoundBytes());
        Assert.assertEquals(0L, maximum.retainedSourceBytes());
    }

    @Test
    public void invalidLengthsAreRejected() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ReplyPlans.bulkString(-2, 0L));
        Assert.assertThrows(IllegalArgumentException.class, () -> ReplyPlans.bulkString(1, -1L));
        Assert.assertThrows(IllegalArgumentException.class, () -> ReplyPlans.bulkStringArray(-2, 0L, 0L));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ReplyPlan(-1L, 0L, false));
    }
}
