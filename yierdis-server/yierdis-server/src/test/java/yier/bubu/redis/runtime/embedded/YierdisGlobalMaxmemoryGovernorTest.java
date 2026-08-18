package yier.bubu.redis.runtime.embedded;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.KeyHandle;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class YierdisGlobalMaxmemoryGovernorTest {
    @Test
    public void prepareWriteThrowsOomUnderNoevictionWhenGrowthWriteAndFull() {
        long maxmemoryBytes = 100;
        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(maxmemoryBytes);
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
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                0
        );

        try {
            governor.prepareWrite(null, 1);
            Assert.fail("expected OOM");
        } catch (RuntimeException e) {
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
        }
    }

    @Test
    public void prepareWriteUsesPhysicalMemoryUsageSnapshotsForAdmission() {
        long maxmemoryBytes = 60;
        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return new MemoryUsageSnapshot(10, 20, 30, 0, 0);
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
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                0
        );

        try {
            governor.prepareWrite(null, 1);
            Assert.fail("expected OOM when physical snapshot exceeds admitted limit");
        } catch (RuntimeException e) {
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
        }
    }

    @Test
    public void prepareWriteTrimsReclaimableMemoryAcrossDatabasesBeforeOom() {
        long maxmemoryBytes = 100;
        AtomicLong reclaimableUsedBytes = new AtomicLong(90);
        AtomicInteger trimCalls = new AtomicInteger(0);

        MaxmemoryParticipant reclaimable = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(reclaimableUsedBytes.get());
            }

            @Override
            public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
                trimCalls.incrementAndGet();
                long before = reclaimableUsedBytes.getAndSet(20);
                return new MemoryReclaimResult(1, 1, Math.max(0L, before - 20), MemoryReclaimResult.StopReason.COMPLETE);
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

        MaxmemoryParticipant requester = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(20);
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
                new MaxmemoryParticipant[]{reclaimable, requester},
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                0
        );

        governor.prepareWrite(null, 10);

        Assert.assertTrue("global admission must pressure-trim all participants before OOM", trimCalls.get() > 0);
        Assert.assertEquals(20L, reclaimableUsedBytes.get());
    }

    @Test
    public void prepareWriteUsesBoundedTrimBudgetWithMinimumInspectionAllowance() {
        AtomicLong usedBytes = new AtomicLong(90L);
        AtomicReference<MemoryPressureBudget> observedBudget = new AtomicReference<>();
        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(usedBytes.get());
            }

            @Override
            public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
                observedBudget.set(budget);
                usedBytes.set(0L);
                return new MemoryReclaimResult(1L, 1L, 90L, MemoryReclaimResult.StopReason.COMPLETE);
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
                100L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                1_000_000_000L
        );

        governor.prepareWrite(null, 10L);

        Assert.assertNotNull(observedBudget.get());
        Assert.assertEquals(16L, observedBudget.get().maxInspectedUnits());
        Assert.assertTrue(observedBudget.get().timeLimitNanos() > 0L);
        Assert.assertTrue(observedBudget.get().timeLimitNanos() < Long.MAX_VALUE);
    }

    @Test
    public void prepareWriteEvictsUntilUnderLimitWhenEvictionSucceeds() {
        long maxmemoryBytes = 100;
        long extraBytes = 10;
        long evictBytesEachTime = 30;

        AtomicLong usedBytes = new AtomicLong(150);
        AtomicInteger evictions = new AtomicInteger(0);

        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(usedBytes.get());
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
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                0
        );

        governor.prepareWrite(null, extraBytes);

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

        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(usedBytes.get());
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
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                0
        );

        governor.prepareWrite(null, extraBytes);

        long limitBytes = maxmemoryBytes - extraBytes;
        Assert.assertTrue("must evict until under limit", usedBytes.get() <= limitBytes);
        Assert.assertTrue("must evict beyond 64 attempts", evictions.get() > 64);
    }

    @Test
    public void prepareWriteStopsWhenEvictionMakesNoPhysicalProgress() {
        long maxmemoryBytes = 100;
        long extraBytes = 10;

        AtomicInteger evictions = new AtomicInteger(0);

        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(150);
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
                return true;
            }
        };

        YierdisGlobalMaxmemoryGovernor governor = new YierdisGlobalMaxmemoryGovernor(
                new MaxmemoryParticipant[]{participant},
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                0
        );

        try {
            governor.prepareWrite(null, extraBytes);
            Assert.fail("expected OOM after eviction fails to reduce physical usage");
        } catch (RuntimeException e) {
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
        }

        Assert.assertEquals("must not repeat a full candidate pass without physical progress", 1, evictions.get());
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

        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(usedBytes.get());
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
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                evictionTimeLimitNanos
        );

        governor.prepareWrite(null, extraBytes);

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

        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(usedBytes.get());
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
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_LRU,
                5,
                0
        );

        governor.prepareWrite(null, extraBytes);

        Assert.assertEquals("sampling should not be used", 0, sampled.get());
        Assert.assertArrayEquals("must evict expected key", expectedKey, evictedKey.get());
    }

    private abstract static class Participant implements MaxmemoryParticipant {
        @Override
        public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
            return MemoryReclaimResult.EMPTY;
        }

        @Override
        public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
            return null;
        }

    }

    private static KeyHandle handle(byte[] key) {
        return new KeyHandle() {
            @Override
            public int length() {
                return key.length;
            }

            @Override
            public byte getByte(int index) {
                return key[index];
            }

        };
    }

    private static byte[] bytes(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.length()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.getByte(i);
        }
        return out;
    }

    private static MemoryUsageSnapshot snapshot(long usedBytes) {
        return new MemoryUsageSnapshot(Math.max(0L, usedBytes), 0L, 0L, 0L, 0L);
    }

    @Test
    public void prepareWriteDoesNotThrowUnderNoevictionForNoGrowthWrites() {
        long maxmemoryBytes = 100;

        MaxmemoryParticipant participant = new Participant() {
            @Override
            public MemoryUsageSnapshot memoryUsage() {
                return snapshot(maxmemoryBytes + 1);
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
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                0
        );

        governor.prepareWrite(null, 0);
    }

}
