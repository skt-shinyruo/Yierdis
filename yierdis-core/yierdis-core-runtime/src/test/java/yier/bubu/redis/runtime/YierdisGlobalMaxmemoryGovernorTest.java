package yier.bubu.redis.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.ops.MaxmemoryCandidate;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.ops.MaxmemoryParticipant;
import yier.bubu.redis.ops.MaxmemoryPolicy;
import yier.bubu.redis.ops.MaxmemoryUsageSource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class YierdisGlobalMaxmemoryGovernorTest {
    @Test
    public void prepareWriteThrowsOomUnderNoevictionWhenGrowthWriteAndFull() {
        long maxmemoryBytes = 100;
        MaxmemoryParticipant participant = new MaxmemoryParticipant() {
            @Override
            public long usedBytesForMaxmemory() {
                return maxmemoryBytes;
            }

            @Override
            public int keyCountEstimate() {
                return 0;
            }

            @Override
            public void cleanupExpired(long nowMillis) {
            }

            @Override
            public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
                return null;
            }

            @Override
            public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
                return false;
            }
        };

        YierdisGlobalMaxmemoryGovernor governor = new YierdisGlobalMaxmemoryGovernor(
                new MaxmemoryParticipant[]{participant},
                new MaxmemoryUsageSource[0],
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                0
        );

        try {
            governor.prepareWrite(1);
            Assert.fail("expected OOM");
        } catch (RuntimeException e) {
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
        }
    }

    @Test
    public void prepareWriteEvictsUntilUnderLimitWhenEvictionSucceeds() {
        long maxmemoryBytes = 100;
        long extraBytes = 10;
        long evictBytesEachTime = 30;

        AtomicLong usedBytes = new AtomicLong(150);
        AtomicInteger evictions = new AtomicInteger(0);

        MaxmemoryParticipant participant = new MaxmemoryParticipant() {
            @Override
            public long usedBytesForMaxmemory() {
                return usedBytes.get();
            }

            @Override
            public int keyCountEstimate() {
                return 1;
            }

            @Override
            public void cleanupExpired(long nowMillis) {
            }

            @Override
            public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
                return new MaxmemoryCandidate(this, new byte[]{'k'}, 0);
            }

            @Override
            public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
                evictions.incrementAndGet();
                long after = usedBytes.addAndGet(-evictBytesEachTime);
                if (after < 0) {
                    usedBytes.set(0);
                }
                return true;
            }
        };

        YierdisGlobalMaxmemoryGovernor governor = new YierdisGlobalMaxmemoryGovernor(
                new MaxmemoryParticipant[]{participant},
                new MaxmemoryUsageSource[0],
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                0
        );

        governor.prepareWrite(extraBytes);

        long limitBytes = maxmemoryBytes - extraBytes;
        Assert.assertTrue("must evict until under limit", usedBytes.get() <= limitBytes);
        Assert.assertEquals("evictions should be minimal", 2, evictions.get());
    }
}
