package yier.bubu.redis.storage.api;

import org.junit.Assert;
import org.junit.Test;

public class MaxmemoryEvictionAttemptsTest {
    @Test
    public void maxAttempts_floorsAt64AndClampsBeforeIntOverflow() {
        Assert.assertEquals(64, MaxmemoryEvictionAttempts.maxAttempts(0));
        Assert.assertEquals(64, MaxmemoryEvictionAttempts.maxAttempts(32));
        Assert.assertEquals(2_000, MaxmemoryEvictionAttempts.maxAttempts(1_000));

        int atOverflowBoundary = Integer.MAX_VALUE / 2;
        Assert.assertEquals(
                (int) ((long) atOverflowBoundary * 2L),
                MaxmemoryEvictionAttempts.maxAttempts(atOverflowBoundary)
        );
        Assert.assertEquals(
                Integer.MAX_VALUE,
                MaxmemoryEvictionAttempts.maxAttempts(atOverflowBoundary + 1)
        );
    }
}
