package yier.bubu.redis.runtime.embedded;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import yier.bubu.redis.storage.api.KeyHandle;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MaxmemoryUsageSource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
                return new MaxmemoryCandidate(this, handle(new byte[]{'k'}), 0);
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

    @Test
    public void prepareWriteDoesNotOverflowMaxAttemptsWhenKeyCountEstimateSaturates() {
        long maxmemoryBytes = 100;
        long extraBytes = 1;

        // Need > 64 evictions to get under limitBytes, so the maxAttempts calculation matters.
        AtomicLong usedBytes = new AtomicLong(200);
        AtomicInteger evictions = new AtomicInteger(0);

        MaxmemoryParticipant participant = new MaxmemoryParticipant() {
            @Override
            public long usedBytesForMaxmemory() {
                return usedBytes.get();
            }

            @Override
            public int keyCountEstimate() {
                return Integer.MAX_VALUE;
            }

            @Override
            public void cleanupExpired(long nowMillis) {
            }

            @Override
            public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
                return new MaxmemoryCandidate(this, handle(new byte[]{'k'}), 0);
            }

            @Override
            public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
                evictions.incrementAndGet();
                long after = usedBytes.decrementAndGet();
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
        Assert.assertTrue("must evict beyond 64 attempts", evictions.get() > 64);
    }

    @Test
    public void prepareWriteDoesNotStopEvictingWhenDeadlineAdditionOverflows() {
        long maxmemoryBytes = 100;
        long extraBytes = 10;

        AtomicLong usedBytes = new AtomicLong(150);
        AtomicInteger evictions = new AtomicInteger(0);

        long nowNanos = System.nanoTime();
        Assume.assumeTrue("nanoTime must be > 0 to reproduce overflow", nowNanos > 0);
        long evictionTimeLimitNanos = Long.MAX_VALUE - nowNanos + 1;

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
                return new MaxmemoryCandidate(this, handle(new byte[]{'k'}), 0);
            }

            @Override
            public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
                evictions.incrementAndGet();
                usedBytes.set(0);
                return true;
            }
        };

        YierdisGlobalMaxmemoryGovernor governor = new YierdisGlobalMaxmemoryGovernor(
                new MaxmemoryParticipant[]{participant},
                new MaxmemoryUsageSource[0],
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                evictionTimeLimitNanos
        );

        governor.prepareWrite(extraBytes);

        long limitBytes = maxmemoryBytes - extraBytes;
        Assert.assertTrue("must evict until under limit", usedBytes.get() <= limitBytes);
        Assert.assertEquals("should evict once", 1, evictions.get());
    }

    @Test
    public void prepareWritePrefersDeterministicScanUnderAllkeysLruWhenSamplesCoverAllKeys() {
        long maxmemoryBytes = 100;
        long extraBytes = 10;

        byte[] expectedKey = new byte[]{'a'};
        AtomicLong usedBytes = new AtomicLong(150);
        AtomicReference<byte[]> evictedKey = new AtomicReference<>(null);
        AtomicInteger sampled = new AtomicInteger(0);

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
                sampled.incrementAndGet();
                throw new AssertionError("sampleCandidate should not be used when scanBestCandidate returns a victim");
            }

            @Override
            public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
                return new MaxmemoryCandidate(this, handle(expectedKey), 0);
            }

            @Override
            public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
                evictedKey.set(bytes(candidate.keyHandle()));
                usedBytes.set(0);
                return true;
            }
        };

        YierdisGlobalMaxmemoryGovernor governor = new YierdisGlobalMaxmemoryGovernor(
                new MaxmemoryParticipant[]{participant},
                new MaxmemoryUsageSource[0],
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_LRU,
                5,
                0
        );

        governor.prepareWrite(extraBytes);

        Assert.assertEquals("sampling should not be used", 0, sampled.get());
        Assert.assertArrayEquals("must evict expected key", expectedKey, evictedKey.get());
    }

    private static KeyHandle handle(byte[] key) {
        return new KeyHandle() {
            @Override
            public int len() {
                return key.length;
            }

            @Override
            public byte byteAt(int index) {
                return key[index];
            }

            @Override
            public int dictHash() {
                int h = 1;
                for (byte b : key) {
                    h = 31 * h + b;
                }
                return h;
            }
        };
    }

    private static byte[] bytes(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.len()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
    }

    @Test
    public void prepareWriteDoesNotThrowUnderNoevictionForNoGrowthWrites() {
        long maxmemoryBytes = 100;

        MaxmemoryParticipant participant = new MaxmemoryParticipant() {
            @Override
            public long usedBytesForMaxmemory() {
                return maxmemoryBytes + 1;
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

        governor.prepareWrite(0);
    }

    @Test
    public void prepareWriteCountsSharedUsageSourcesInOomDecision() {
        long maxmemoryBytes = 100;

        MaxmemoryUsageSource sharedUsage = () -> maxmemoryBytes;

        MaxmemoryParticipant participant = new MaxmemoryParticipant() {
            @Override
            public long usedBytesForMaxmemory() {
                return 0;
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
                new MaxmemoryUsageSource[]{sharedUsage},
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
}
