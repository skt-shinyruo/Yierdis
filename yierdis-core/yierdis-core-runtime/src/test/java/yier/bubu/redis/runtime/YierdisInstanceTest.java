package yier.bubu.redis.runtime;

// YierdisInstanceTest：覆盖可嵌入 instance 的装配语义与关键不变量（多 DB + global maxmemory + shared off-heap 等）。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;
import yier.bubu.redis.ops.DbEngineFactory;
import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.RuntimeDbEngine;
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
                .maxmemoryPolicy("noeviction")
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
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
                    String maxmemoryPolicy,
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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
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
        public void enqueue(byte[][] argv) {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public java.util.List<byte[][]> drain() {
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
