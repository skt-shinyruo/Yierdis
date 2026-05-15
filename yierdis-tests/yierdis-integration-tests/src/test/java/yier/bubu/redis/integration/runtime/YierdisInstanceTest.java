package yier.bubu.redis.integration.runtime;

// YierdisInstanceTest：覆盖可嵌入 instance 的装配语义与关键不变量（多 DB + global maxmemory + shared off-heap 等）。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.integration.command.TestCommandProcessors;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ServerSession;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.embedded.YierdisInstanceMaintenance;
import yier.bubu.redis.runtime.embedded.YierdisInstanceObservability;
import yier.bubu.redis.runtime.embedded.YierdisInstanceRuntimeAccess;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static yier.bubu.redis.testutil.TestBytes.b;

public class YierdisInstanceTest {
    @Test
    public void globalMaxmemoryCountsSharedFfmRuntimeOnceAcrossDbs() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(9000)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = TestCommandProcessors.forRouter(TestDbRouters.forInstance(instance));
            TestSession session = new TestSession();
            byte[] value = new byte[4000];
            Arrays.fill(value, (byte) 'a');

            try (FastTestClient client = new FastTestClient(processor, session)) {
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("k0"), value))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SELECT"), b("1")))).value());

                // 若 shared allocator 被按 DB 重复计入 maxmemory，这里通常会提前 OOM。
                Object reply = client.execute(Arrays.asList(b("SET"), b("k1"), value));
                Assert.assertFalse("expected not OOM (no double-count off-heap)", reply instanceof ReplyError);
                Assert.assertEquals("OK", ((ReplySimpleString) reply).value());
            }
        }
    }

    @Test
    public void perDbScopeDoesNotCountAnotherDbsFfmBytesInLocalBudget() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .maxmemoryBytes(20_000)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            byte[] value = new byte[4_000];
            Arrays.fill(value, (byte) 'a');

            long db0Before = instance.engine(0).memory().memoryStats().usedBytesForMaxmemory();

            Assert.assertTrue(instance.engine(1).writes().strings().setString(b("remote"), value, SetMode.NORMAL, null).value());
            long db0AfterRemoteWrite = instance.engine(0).memory().memoryStats().usedBytesForMaxmemory();
            Assert.assertEquals("DB1 writes must not consume DB0 maxmemory budget in per-db mode",
                    db0Before,
                    db0AfterRemoteWrite);

            Assert.assertTrue(instance.engine(0).writes().strings().setString(b("local"), value, SetMode.NORMAL, null).value());
            long db0AfterLocalWrite = instance.engine(0).memory().memoryStats().usedBytesForMaxmemory();
            Assert.assertTrue("DB0 local writes should increase its own maxmemory usage",
                    db0AfterLocalWrite > db0AfterRemoteWrite);
        }
    }

    @Test
    public void instanceObservabilitySumsOffHeapInPerDbScopeAndMarksIncluded() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .maxmemoryBytes(0)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            byte[] value = new byte[4_000];
            Arrays.fill(value, (byte) 'a');

            Assert.assertTrue(instance.engine(0).writes().strings().setString(b("a"), value, SetMode.NORMAL, null).value());
            Assert.assertTrue(instance.engine(1).writes().strings().setString(b("b"), value, SetMode.NORMAL, null).value());

            long off0 = instance.engine(0).memory().memoryStats().offHeapUsedBytes();
            long off1 = instance.engine(1).memory().memoryStats().offHeapUsedBytes();
            long expected;
            if (Long.MAX_VALUE - off0 < off1) {
                expected = Long.MAX_VALUE;
            } else {
                expected = off0 + off1;
            }

            var stats = instance.observability().memoryStats();
            Assert.assertTrue("expected off-heap to be included in maxmemory accounting", stats.offHeapIncludedInMaxmemory());
            Assert.assertEquals("expected instance off-heap to sum across DBs in per-db scope", expected, stats.offHeapUsedBytes());
        }
    }

    @Test
    public void instanceObservabilityUsesSharedRuntimeOffHeapInGlobalScope() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(0)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            byte[] value = new byte[4_000];
            Arrays.fill(value, (byte) 'a');

            Assert.assertTrue(instance.engine(0).writes().strings().setString(b("a"), value, SetMode.NORMAL, null).value());
            Assert.assertTrue(instance.engine(1).writes().strings().setString(b("b"), value, SetMode.NORMAL, null).value());

            long off0 = instance.engine(0).memory().memoryStats().offHeapUsedBytes();
            long off1 = instance.engine(1).memory().memoryStats().offHeapUsedBytes();

            var stats = instance.observability().memoryStats();
            Assert.assertTrue("expected off-heap to be included in global maxmemory accounting", stats.offHeapIncludedInMaxmemory());
            long expectedLogicalOffHeap = addSaturating(off0, off1);
            Assert.assertEquals("expected global observability to sum actual DB off-heap usage",
                    expectedLogicalOffHeap,
                    stats.offHeapUsedBytes());
            Assert.assertEquals("global maxmemory usage should use the same off-heap total exposed in stats",
                    stats.heapDataBytesEstimate() + stats.offHeapUsedBytes(),
                    stats.usedBytesForMaxmemory());
        }
    }

    @Test
    public void nativeDefragConfigEnablesDefaultFactoryMaintenance() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .nativeDefragEnabled(true)
                .nativeDefragMaxMoveBytes(1_000_000)
                .nativeDefragMaxObjects(1_000)
                .nativeDefragTimeLimitMillis(1000)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            Assert.assertTrue(instance.engine(0).writes().strings().setString(b("k"), b("value"), SetMode.NORMAL, null).value());

            new YierdisInstanceMaintenance(instance).maintenanceTick();

            Assert.assertArrayEquals(b("value"), instance.engine(0).reads().strings().getStringBytes(b("k")));
            Assert.assertTrue(instance.engine(0).memory().memoryStats().nativeDefragLastMovedObjects() > 0L);
        }
    }

    @Test
    public void maxmemoryPolicyBuilderUsesDomainEnumAndKeepsStringCompatibility() {
        YierdisInstanceConfig typed = YierdisInstanceConfig.builder()
                .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
                .build();
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_LRU, typed.maxmemoryPolicy());

        YierdisInstanceConfig defaulted = YierdisInstanceConfig.builder()
                .maxmemoryPolicy((MaxmemoryPolicy) null)
                .build();
        Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, defaulted.maxmemoryPolicy());

        YierdisInstanceConfig legacyString = YierdisInstanceConfig.builder()
                .maxmemoryPolicy("ALLKEYS_RANDOM")
                .build();
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, legacyString.maxmemoryPolicy());

        YierdisInstanceConfig legacyBlank = YierdisInstanceConfig.builder()
                .maxmemoryPolicy(" ")
                .build();
        Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, legacyBlank.maxmemoryPolicy());
    }

    private static long addSaturating(long left, long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    @Test
    public void unboundOrCrossThreadAccessFailsFast() throws Exception {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder().build();
        try (YierdisInstance instance = YierdisInstance.create(config)) {
            try {
                instance.engine(0).memory().memoryStats();
                Assert.fail("expected fail-fast before bind");
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("bindToCurrentThread"));
            }

            instance.bindToCurrentThread();

            AtomicReference<Throwable> errRef = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try {
                    instance.engine(0).memory().memoryStats();
                } catch (Throwable t1) {
                    errRef.set(t1);
                }
            });
            t.start();
            t.join();

            Throwable err = errRef.get();
            Assert.assertNotNull("expected error from non-owner thread", err);
            Assert.assertTrue("expected IllegalStateException", err instanceof IllegalStateException);
            Assert.assertTrue(err.getMessage().contains("non-owner thread"));
        }
    }

    @Test
    public void closePropagatesDbAndAllocatorFailuresAfterBestEffortCleanup() {
        List<String> closeOrder = new ArrayList<>();
        DbEngineFactory factory = (dbIndex,
                                   maxmemoryBytes,
                                   maxmemoryPolicy,
                                   maxmemorySamples,
                                   evictionTimeLimitMillis,
                                   expireCleanupTimeLimitMillis) -> new FailingRuntimeDbEngine("db-" + dbIndex, closeOrder);

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory(factory)
                .build());

        try {
            instance.close();
            Assert.fail("expected close failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("db-0", e.getMessage());
            Assert.assertEquals(Arrays.asList("db-0", "db-1"), closeOrder);
            Assert.assertEquals(1, e.getSuppressed().length);
            Assert.assertEquals("db-1", e.getSuppressed()[0].getMessage());
        }
    }

    @Test
    public void createCleansUpAlreadyCreatedEnginesWhenFactoryFailsMidStartup() {
        List<String> closeOrder = new ArrayList<>();
        DbEngineFactory factory = new DbEngineFactory() {
            private int calls;

            @Override
            public RuntimeDbEngine create(
                    int dbIndex,
                    long maxmemoryBytes,
                    MaxmemoryPolicy maxmemoryPolicy,
                    int maxmemorySamples,
                    long evictionTimeLimitMillis,
                    long expireCleanupTimeLimitMillis
            ) {
                if (calls++ == 0) {
                    return new CloseTrackingRuntimeDbEngine("db-" + dbIndex, closeOrder);
                }
                throw new IllegalStateException("boom-create-" + dbIndex);
            }
        };

        try {
            YierdisInstance.create(YierdisInstanceConfig.builder()
                    .databases(2)
                    .engineFactory(factory)
                    .build());
            Assert.fail("expected startup failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("boom-create-1", e.getMessage());
            Assert.assertEquals(Arrays.asList("db-0"), closeOrder);
        }
    }

    @Test
    public void runtimeAccessExposesOwnerThreadLifecycleHooks() {
        TrackingRuntimeDbEngine engine = new TrackingRuntimeDbEngine();
        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                .databases(1)
                .engineFactory((dbIndex,
                                maxmemoryBytes,
                                maxmemoryPolicy,
                                maxmemorySamples,
                                evictionTimeLimitMillis,
                                expireCleanupTimeLimitMillis) -> engine)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .maxmemoryBytes(128)
                .build());

        try {
            YierdisInstanceRuntimeAccess runtimeAccess = instance.runtimeAccess();
            runtimeAccess.bindToCurrentThread();
            runtimeAccess.maintenanceTick();
            runtimeAccess.close();
        } finally {
            try {
                instance.close();
            } catch (Throwable ignored) {
            }
        }

        Assert.assertEquals(1, engine.bindCalls);
        Assert.assertEquals(1, engine.expirationCleanupCalls);
        Assert.assertEquals(1, engine.defragMaintenanceCalls);
        Assert.assertEquals(1, engine.maxmemoryMaintenanceCalls);
        Assert.assertEquals(1, engine.shutdownCalls);
    }

    @Test
    public void maintenanceUsesRuntimeMaxmemoryHookForPerDbScope() {
        TrackingRuntimeDbEngine engine = new TrackingRuntimeDbEngine();
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .engineFactory((dbIndex,
                                maxmemoryBytes,
                                maxmemoryPolicy,
                                maxmemorySamples,
                                evictionTimeLimitMillis,
                                expireCleanupTimeLimitMillis) -> engine)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .maxmemoryBytes(128)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            new YierdisInstanceMaintenance(instance).maintenanceTick();
        }

        Assert.assertEquals(1, engine.expirationCleanupCalls);
        Assert.assertEquals(1, engine.defragMaintenanceCalls);
        Assert.assertEquals(1, engine.maxmemoryMaintenanceCalls);
    }

    @Test
    public void globalMaintenanceUsesGovernorInsteadOfFirstEngineRuntimeHook() {
        TrackingRuntimeDbEngine engine0 = new TrackingRuntimeDbEngine();
        TrackingRuntimeDbEngine engine1 = new TrackingRuntimeDbEngine();
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory((dbIndex,
                                maxmemoryBytes,
                                maxmemoryPolicy,
                                maxmemorySamples,
                                evictionTimeLimitMillis,
                                expireCleanupTimeLimitMillis) -> dbIndex == 0 ? engine0 : engine1)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(128)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            new YierdisInstanceMaintenance(instance).maintenanceTick();
        }

        Assert.assertEquals(1, engine0.expirationCleanupCalls);
        Assert.assertEquals(1, engine1.expirationCleanupCalls);
        Assert.assertEquals(1, engine0.defragMaintenanceCalls);
        Assert.assertEquals(1, engine1.defragMaintenanceCalls);
        Assert.assertEquals(0, engine0.maxmemoryMaintenanceCalls);
        Assert.assertEquals(0, engine1.maxmemoryMaintenanceCalls);
        Assert.assertEquals(1, engine0.participantCleanupCalls);
        Assert.assertEquals(1, engine1.participantCleanupCalls);
    }

    @Test
    public void observabilityDbSummariesExposePerDbKeysAndExpires() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .build();
        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = TestCommandProcessors.forRouter(TestDbRouters.forInstance(instance));
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("k0"), b("v0")))).value());
                Assert.assertEquals(1L, ((ReplyInteger) client.execute(Arrays.asList(b("EXPIRE"), b("k0"), b("60")))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SELECT"), b("1")))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("k1"), b("v1")))).value());
            }

            List<YierdisInstanceObservability.YierdisDbSummary> summaries = instance.observability().dbSummaries();
            Assert.assertEquals(2, summaries.size());
            Assert.assertEquals(0, summaries.get(0).dbIndex());
            Assert.assertEquals(1, summaries.get(0).keyCount());
            Assert.assertEquals(1, summaries.get(0).expireCount());
            Assert.assertEquals(1, summaries.get(1).dbIndex());
            Assert.assertEquals(1, summaries.get(1).keyCount());
            Assert.assertEquals(0, summaries.get(1).expireCount());
        }
    }

    private static final class TestSession implements ServerSession {
        private int dbIndex;
        private String clientName;
        private boolean authenticated;
        private final TransactionState tx = new NoopTransactionState();

        @Override
        public int dbIndex() {
            return dbIndex;
        }

        @Override
        public void setDbIndex(int dbIndex) {
            this.dbIndex = Math.max(0, dbIndex);
        }

        @Override
        public long clientId() {
            return 1L;
        }

        @Override
        public String clientName() {
            return clientName;
        }

        @Override
        public void setClientName(String clientName) {
            this.clientName = clientName;
        }

        @Override
        public boolean authenticated() {
            return authenticated;
        }

        @Override
        public void setAuthenticated(boolean authenticated) {
            this.authenticated = authenticated;
        }

        @Override
        public TransactionState transaction() {
            return tx;
        }

        @Override
        public yier.bubu.redis.execution.api.ConnectionStatsView connectionStats() {
            return null;
        }
    }

    private static final class NoopTransactionState implements TransactionState {
        @Override
        public boolean active() {
            return false;
        }

        @Override
        public void begin() {
        }

        @Override
        public void discard() {
        }

        @Override
        public void enqueue(ExecutionRequest request) {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public java.util.List<ExecutionRequest> drain() {
            return java.util.Collections.emptyList();
        }
    }

    private static final class CloseTrackingRuntimeDbEngine implements RuntimeDbEngine {
        private final String name;
        private final List<String> closeOrder;

        private CloseTrackingRuntimeDbEngine(String name, List<String> closeOrder) {
            this.name = name;
            this.closeOrder = closeOrder;
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void enforceMaxmemoryMaintenance() {
        }

        @Override
        public void shutdown() {
            closeOrder.add(name);
        }

        @Override
        public DbReads reads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbWrites writes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryOps memory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FailingRuntimeDbEngine implements RuntimeDbEngine {
        private final String name;
        private final List<String> closeOrder;

        private FailingRuntimeDbEngine(String name, List<String> closeOrder) {
            this.name = name;
            this.closeOrder = closeOrder;
        }

        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void enforceMaxmemoryMaintenance() {
        }

        @Override
        public void shutdown() {
            closeOrder.add(name);
            throw new IllegalStateException(name);
        }

        @Override
        public DbReads reads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbWrites writes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryOps memory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TrackingRuntimeDbEngine implements RuntimeDbEngine {
        private int bindCalls;
        private int expirationCleanupCalls;
        private int defragMaintenanceCalls;
        private int maxmemoryMaintenanceCalls;
        private int shutdownCalls;
        private int participantCleanupCalls;

        @Override
        public void bindToCurrentThread() {
            bindCalls++;
        }

        @Override
        public void enforceMaxmemoryMaintenance() {
            maxmemoryMaintenanceCalls++;
        }

        @Override
        public void defragMaintenance() {
            defragMaintenanceCalls++;
        }

        @Override
        public void shutdown() {
            shutdownCalls++;
        }

        @Override
        public void cleanupExpired(long nowMillis) {
            participantCleanupCalls++;
        }

        @Override
        public DbReads reads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbWrites writes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            return () -> expirationCleanupCalls++;
        }

        @Override
        public MemoryOps memory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }
}
