package yier.bubu.redis.runtime.embedded;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.storage.api.*;

public class YierdisInstanceCapabilityValidationTest {
    @Test
    public void nullEngineFailsAndClosesPreviouslyCreatedEngine() {
        BaselineEngine first = new BaselineEngine();
        Factory factory = new Factory(first, null);

        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(2, factory).build())
        );

        Assert.assertTrue(failure.getMessage().contains("dbIndex=1"));
        Assert.assertEquals(1, first.shutdownCalls.get());
    }

    @Test
    public void mixedCapabilityVectorsFailBeforeAnyAttachment() {
        CommitEngine first = new CommitEngine();
        BaselineEngine second = new BaselineEngine();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(2, new Factory(first, second)).build())
        );

        Assert.assertEquals(0, first.attachCalls.get());
        Assert.assertEquals(1, first.shutdownCalls.get());
        Assert.assertEquals(1, second.shutdownCalls.get());
    }

    @Test
    public void configuredChangeSinkRequiresCommitCapabilityBeforeStreamStart() {
        BaselineEngine engine = new BaselineEngine();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(1, new Factory(engine))
                        .changeSink(event -> { })
                        .build())
        );

        Assert.assertEquals(1, engine.shutdownCalls.get());
    }

    @Test
    public void publisherAttachmentFailurePreservesCauseAndShutsDownEveryEngineOnce() {
        CommitEngine first = new CommitEngine();
        IllegalStateException attachFailure = new IllegalStateException("boom-attach");
        CommitEngine second = new CommitEngine(attachFailure);

        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(2, new Factory(first, second))
                        .changeSink(event -> { })
                        .build())
        );

        Assert.assertSame(attachFailure, failure);
        Assert.assertEquals(1, first.attachCalls.get());
        Assert.assertEquals(1, second.attachCalls.get());
        Assert.assertEquals(1, first.shutdownCalls.get());
        Assert.assertEquals(1, second.shutdownCalls.get());
    }

    @Test
    public void globalMaxmemoryRequiresGlobalCapability() {
        BaselineEngine engine = new BaselineEngine();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(1, new Factory(engine))
                        .maxmemoryBytes(1024L)
                        .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                        .build())
        );
    }

    @Test
    public void enabledDefragRequiresDefragCapability() {
        BaselineEngine engine = new BaselineEngine();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(1, new Factory(engine))
                        .defrag(new DbDefragConfig(true, 64L * 1024L, 64L, 1L))
                        .build())
        );
    }

    @Test
    public void validCapabilitiesReceiveConfigurationBeforeUse() {
        AllCapabilitiesEngine engine = new AllCapabilitiesEngine();
        Factory factory = new Factory(engine);

        try (YierdisInstance ignored = YierdisInstance.create(baseConfig(1, factory)
                .changeSink(event -> { })
                .maxmemoryBytes(1024L)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .defrag(new DbDefragConfig(true, 2048L, 4L, 6L))
                .build())) {
            Assert.assertEquals(1, engine.attachCalls.get());
            Assert.assertEquals(1, factory.configs.size());
            Assert.assertEquals(new DbDefragConfig(true, 2048L, 4L, 6L), factory.configs.get(0).defrag());
        }
    }

    private static YierdisInstanceConfig.Builder baseConfig(int databases, DbEngineFactory factory) {
        return YierdisInstanceConfig.builder().databases(databases).engineFactory(factory);
    }

    private static class Factory implements DbEngineFactory {
        private final RuntimeDbEngine[] engines;
        private final List<DbEngineConfig> configs = new ArrayList<>();
        private int next;

        private Factory(RuntimeDbEngine... engines) {
            this.engines = engines;
        }

        @Override
        public RuntimeDbEngine create(DbEngineConfig config) {
            configs.add(config);
            return engines[next++];
        }
    }

    private static class BaselineEngine implements RuntimeDbEngine {
        final AtomicInteger shutdownCalls = new AtomicInteger();
        @Override public DbReads reads() { return null; }
        @Override public DbWrites writes() { return null; }
        @Override public MemoryOps memory() { return null; }
        @Override public DbLifecycleOps lifecycle() { return null; }
        @Override public void bindToCurrentThread() { }
        @Override public void runMaintenance() { }
        @Override public void shutdown() { shutdownCalls.incrementAndGet(); }
    }

    private static class CommitEngine extends BaselineEngine implements CommitPublishingDbEngine {
        final AtomicInteger attachCalls = new AtomicInteger();
        private final RuntimeException attachFailure;

        private CommitEngine() {
            this(null);
        }

        private CommitEngine(RuntimeException attachFailure) {
            this.attachFailure = attachFailure;
        }

        @Override public void attachCommitPublisher(DbCommitPublisher publisher, int dbIndex) {
            attachCalls.incrementAndGet();
            if (attachFailure != null) {
                throw attachFailure;
            }
        }
    }

    private static final class AllCapabilitiesEngine extends CommitEngine
            implements GlobalMaxmemoryDbEngine, DefragmentableDbEngine {
        @Override public MemoryUsageSnapshot memoryUsage() {
            return new MemoryUsageSnapshot(0L, 0L, 0L, 0L, 0L);
        }
        @Override public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
            return MemoryReclaimResult.empty();
        }
        @Override public int keyCountEstimate() { return 0; }
        @Override public void cleanupExpired(long nowMillis) { }
        @Override public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) { return null; }
        @Override public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) { return null; }
        @Override public boolean evict(MaxmemoryCandidate candidate, long nowMillis) { return false; }
        @Override public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) { }
        @Override public void defragMaintenance() { }
    }
}
