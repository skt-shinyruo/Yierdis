package yier.bubu.redis.integration.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.runtime.embedded.YierdisGlobalMaxmemoryGovernor;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.KeyHandle;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;

import java.util.concurrent.atomic.AtomicInteger;

public class MaxmemoryPhysicalProgressTest {
    @Test
    public void globalEvictionStopsWhenVictimsDoNotReducePhysicalUsage() {
        AtomicInteger evictions = new AtomicInteger(0);
        GlobalMaxmemoryDbEngine participant = new PhysicalProgressEngine(evictions);
        YierdisGlobalMaxmemoryGovernor governor = new YierdisGlobalMaxmemoryGovernor(
                new GlobalMaxmemoryDbEngine[]{participant},
                100,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                0
        );

        try {
            governor.prepareWrite(null, 10);
            Assert.fail("expected OOM when eviction makes no physical progress");
        } catch (RuntimeException e) {
            Assert.assertEquals(MaxmemoryErrors.OOM_ERR, e.getMessage());
        }

        Assert.assertEquals("one full candidate pass should be enough for a single-key DB", 1, evictions.get());
    }

    private static final class PhysicalProgressEngine implements GlobalMaxmemoryDbEngine {
        private final AtomicInteger evictions;

        private PhysicalProgressEngine(AtomicInteger evictions) {
            this.evictions = evictions;
        }

        @Override
        public DbReads reads() {
            return null;
        }

        @Override
        public DbWrites writes() {
            return null;
        }

        @Override
        public MemoryOps memory() {
            return null;
        }

        @Override
        public DbLifecycleOps lifecycle() {
            return null;
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void runMaintenance() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public MemoryUsageSnapshot memoryUsage() {
            return new MemoryUsageSnapshot(150, 0, 0, 0, 0);
        }

        @Override
        public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
            return MemoryReclaimResult.empty();
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
        public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
            return sampleCandidate(policy, nowMillis);
        }

        @Override
        public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
            evictions.incrementAndGet();
            return true;
        }

        @Override
        public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
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
}
