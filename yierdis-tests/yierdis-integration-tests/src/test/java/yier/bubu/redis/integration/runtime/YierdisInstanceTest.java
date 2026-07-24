package yier.bubu.redis.integration.runtime;

// YierdisInstanceTest：覆盖可嵌入 instance 的装配语义与关键不变量（多 DB + global maxmemory + shared off-heap 等）。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.integration.command.TestCommandProcessors;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;
import yier.bubu.redis.storage.api.DefragmentableDbEngine;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.runtime.api.YierdisChangeKind;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.embedded.CommitStreamState;
import yier.bubu.redis.runtime.embedded.YierdisInstanceMaintenance;
import yier.bubu.redis.runtime.embedded.YierdisInstanceObservability;
import yier.bubu.redis.runtime.embedded.YierdisInstanceRuntimeAccess;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;
import yier.bubu.redis.testutil.TestYierdisInstances;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static yier.bubu.redis.testutil.TestBytes.b;

public class YierdisInstanceTest {
    @Test
    public void globalMaxmemoryCountsSharedFfmRuntimeOnceAcrossDbs() {
        byte[] value = new byte[4000];
        Arrays.fill(value, (byte) 'a');
        long maxmemoryBytes = minGlobalMaxmemoryThatAllowsTwoDbSets(value);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = TestCommandProcessors.forRouter(TestDbRouters.forInstance(instance));
            TestSession session = new TestSession();

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
    public void globalNoevictionSetCommandAllowsOverwriteThatShrinksWhenUsedEqualsLimit() {
        byte[] key = b("k");
        byte[] largeValue = new byte[1600];
        Arrays.fill(largeValue, (byte) 'x');
        byte[] smallValue = b("x");
        long maxmemoryBytes = minGlobalMaxmemoryThatAllowsSetAndOverwrite(key, largeValue, smallValue);

        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = TestCommandProcessors.forRouter(TestDbRouters.forInstance(instance));
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), key, largeValue))).value());
                long usedBefore = instance.observability().memoryStats().usedBytesForMaxmemory();
                Assert.assertTrue(usedBefore <= maxmemoryBytes);

                ReplyObject reply = client.execute(Arrays.asList(b("SET"), key, smallValue));

                Assert.assertFalse("shrinking overwrite must not be rejected by global noeviction", reply instanceof ReplyError);
                Assert.assertEquals("OK", ((ReplySimpleString) reply).value());
                Assert.assertArrayEquals(smallValue, ((ReplyBulkString) client.execute(Arrays.asList(b("GET"), key))).data());
                Assert.assertTrue("global used bytes should shrink",
                        instance.observability().memoryStats().usedBytesForMaxmemory() < usedBefore);
            }
        }
    }

    @Test
    public void perDbScopeDoesNotCountAnotherDbsFfmBytesInLocalBudget() {
        byte[] value = new byte[4_000];
        Arrays.fill(value, (byte) 'a');
        long perDbMaxmemoryBytes = Math.max(
                minPerDbMaxmemoryThatAllowsSet(0, b("local"), value),
                minPerDbMaxmemoryThatAllowsSet(1, b("remote"), value)
        );
        long maxmemoryBytes = Math.multiplyExact(perDbMaxmemoryBytes, 2L);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();

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

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
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

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
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
                .defrag(new DbDefragConfig(true, 1_000_000L, 1_000L, 1_000L))
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            Assert.assertTrue(instance.engine(0).writes().strings().setString(b("k"), b("value"), SetMode.NORMAL, null).value());

            new YierdisInstanceMaintenance(instance).maintenanceTick();

            Assert.assertArrayEquals(b("value"), instance.engine(0).reads().strings().getStringBytes(b("k")));
            Assert.assertTrue(instance.engine(0).memory().memoryStats().nativeDefragLastMovedObjects() > 0L);
        }
    }

    @Test
    public void maintenanceTickEmitsSyntheticDeleteForExpiredKeys() {
        List<DeliveredChange> events = new ArrayList<>();
        CountDownLatch delivered = new CountDownLatch(3);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .changeSink(event -> {
                    synchronized (events) {
                        events.add(new DeliveredChange(
                                event.sequence(),
                                event.synthetic(),
                                event.kind(),
                                new String(event.request().toByteArray(0), java.nio.charset.StandardCharsets.US_ASCII),
                                new String(event.request().toByteArray(1), java.nio.charset.StandardCharsets.US_ASCII)
                        ));
                    }
                    delivered.countDown();
                })
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            try (ExecutionRequest record = ByteArrayExecutionRequest.fromUtf8("SET", List.of("maintenance-expired", "v"))) {
                Assert.assertTrue(instance.engine(0).writes().withMutationContext(MutationContext.of(record)).strings()
                        .setString(b("maintenance-expired"), b("v"), SetMode.NORMAL, null)
                        .value());
            }
            try (ExecutionRequest record = ByteArrayExecutionRequest.fromUtf8("PEXPIRE", List.of("maintenance-expired", "1"))) {
                Assert.assertTrue(instance.engine(0).writes().withMutationContext(MutationContext.of(record)).ttl()
                        .pexpire(view(b("maintenance-expired")), 1L).value());
            }

            sleepPastTtl();
            new YierdisInstanceMaintenance(instance).maintenanceTick();

            try {
                Assert.assertTrue("commit stream did not deliver expiry", delivered.await(5L, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted while waiting for commit stream");
            }
            assertSyntheticDelete(events, "maintenance-expired", YierdisChangeKind.EXPIRED);
        }
    }

    @Test
    public void commitStreamConfigurationControlsAdmissionAndNoopObservability() throws Exception {
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(
                YierdisInstanceConfig.builder().build()
        )) {
            Assert.assertEquals(CommitStreamState.DISABLED, instance.observability().commitStreamStats().state());
            Assert.assertEquals(0L, instance.observability().commitStreamStats().reservedEvents());
            Assert.assertEquals(0L, instance.observability().commitStreamStats().reservedBytes());
        }

        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .changeSink(event -> {
                    callbackEntered.countDown();
                    try {
                        Assert.assertTrue(releaseCallback.await(2L, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                })
                .commitStreamMaxEvents(1)
                .commitStreamMaxRetainedBytes(1_024L)
                .commitStreamShutdownTimeoutMillis(100L)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            Assert.assertEquals(CommitStreamState.RUNNING, instance.observability().commitStreamStats().state());
            try {
                scopedSet(instance, "first");
                Assert.assertTrue(callbackEntered.await(2L, TimeUnit.SECONDS));

                Assert.assertThrows(DbCommitStreamUnavailableException.class, () -> scopedSet(instance, "second"));
                Assert.assertEquals(1L, instance.observability().commitStreamStats().rejectedWrites());
            } finally {
                releaseCallback.countDown();
            }
        }
    }

    @Test
    public void maxmemoryPolicyBuilderUsesDomainEnum() {
        YierdisInstanceConfig typed = YierdisInstanceConfig.builder()
                .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
                .build();
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_LRU, typed.maxmemoryPolicy());

        YierdisInstanceConfig defaulted = YierdisInstanceConfig.builder()
                .maxmemoryPolicy((MaxmemoryPolicy) null)
                .build();
        Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, defaulted.maxmemoryPolicy());

        YierdisInstanceConfig random = YierdisInstanceConfig.builder()
                .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_RANDOM)
                .build();
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, random.maxmemoryPolicy());
    }

    private static long addSaturating(long left, long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static void scopedSet(YierdisInstance instance, String key) {
        try (ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("SET", List.of(key, "v"))) {
            Assert.assertTrue(instance.engine(0).writes().withMutationContext(MutationContext.of(request)).strings()
                    .setString(b(key), b("v"), SetMode.NORMAL, null).value());
        }
    }

    private static long minGlobalMaxmemoryThatAllowsSetAndOverwrite(
            byte[] key,
            byte[] initialValue,
            byte[] overwriteValue
    ) {
        return minimumAcceptedMaxmemory(
                limit -> allowsGlobalSetAndOverwrite(limit, key, initialValue, overwriteValue)
        );
    }

    private static long minGlobalMaxmemoryThatAllowsTwoDbSets(byte[] value) {
        return minimumAcceptedMaxmemory(limit -> allowsTwoDbGlobalSets(limit, value));
    }

    private static long minPerDbMaxmemoryThatAllowsSet(int dbIndex, byte[] key, byte[] value) {
        return minimumAcceptedMaxmemory(limit -> allowsPerDbSet(limit, dbIndex, key, value));
    }

    private static long minimumAcceptedMaxmemory(MaxmemoryAttempt attempt) {
        long high = 1L;
        while (!attempt.allows(high)) {
            high = Math.multiplyExact(high, 2L);
        }

        long low = 0L;
        while (low + 1L < high) {
            long mid = low + (high - low) / 2L;
            if (attempt.allows(mid)) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static boolean allowsGlobalSetAndOverwrite(
            long maxmemoryBytes,
            byte[] key,
            byte[] initialValue,
            byte[] overwriteValue
    ) {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            instance.engine(0).writes().strings().setString(key, initialValue, SetMode.NORMAL, null);
            instance.engine(0).writes().strings().setString(key, overwriteValue, SetMode.NORMAL, null);
            return true;
        } catch (YierdisCommandException e) {
            return isExpectedOom(e);
        }
    }

    private static boolean allowsTwoDbGlobalSets(long maxmemoryBytes, byte[] value) {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            instance.engine(0).writes().strings().setString(b("k0"), value, SetMode.NORMAL, null);
            instance.engine(1).writes().strings().setString(b("k1"), value, SetMode.NORMAL, null);
            return true;
        } catch (YierdisCommandException e) {
            return isExpectedOom(e);
        }
    }

    private static boolean allowsPerDbSet(long maxmemoryBytes, int dbIndex, byte[] key, byte[] value) {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            instance.engine(dbIndex).writes().strings().setString(key, value, SetMode.NORMAL, null);
            return true;
        } catch (YierdisCommandException e) {
            return isExpectedOom(e);
        }
    }

    private static boolean isExpectedOom(YierdisCommandException e) {
        if (MaxmemoryErrors.OOM_ERR.equals(e.getMessage())) {
            return false;
        }
        throw e;
    }

    @FunctionalInterface
    private interface MaxmemoryAttempt {
        boolean allows(long maxmemoryBytes);
    }

    private static void assertSyntheticDelete(List<DeliveredChange> events, String key, YierdisChangeKind kind) {
        Assert.assertEquals(3, events.size());
        DeliveredChange event = events.get(2);
        Assert.assertTrue(event.synthetic());
        Assert.assertEquals(kind, event.kind());
        Assert.assertEquals("DEL", event.command());
        Assert.assertEquals(key, event.key());
    }

    private static yier.bubu.redis.bytes.BytesView view(byte[] data) {
        return new yier.bubu.redis.bytes.BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }

    private static void sleepPastTtl() {
        try {
            Thread.sleep(20L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("interrupted while waiting for TTL to pass");
        }
    }

    private record DeliveredChange(
            long sequence,
            boolean synthetic,
            YierdisChangeKind kind,
            String command,
            String key
    ) {
    }

    @Test
    public void unboundOrCrossThreadAccessFailsFast() throws Exception {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder().build();
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
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
        DbEngineFactory factory = config -> new FailingRuntimeDbEngine("db-" + config.dbIndex(), closeOrder);

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
            public RuntimeDbEngine create(DbEngineConfig config) {
                if (calls++ == 0) {
                    return new CloseTrackingRuntimeDbEngine("db-" + config.dbIndex(), closeOrder);
                }
                throw new IllegalStateException("boom-create-" + config.dbIndex());
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
        DefragmentingTrackingRuntimeDbEngine engine = new DefragmentingTrackingRuntimeDbEngine();
        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                .databases(1)
                .engineFactory(config -> engine)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .maxmemoryBytes(128)
                .defrag(new DbDefragConfig(true, 0L, 0L, 0L))
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
        Assert.assertEquals(1, engine.runMaintenanceCalls);
        Assert.assertEquals(1, engine.defragMaintenanceCalls);
        Assert.assertEquals(1, engine.shutdownCalls);
    }

    @Test
    public void maintenanceRunsDbLocalTickForPerDbScope() {
        TrackingRuntimeDbEngine engine = new TrackingRuntimeDbEngine();
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(1)
                .engineFactory(ignored -> engine)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .maxmemoryBytes(128)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            new YierdisInstanceMaintenance(instance).maintenanceTick();
        }

        Assert.assertEquals(1, engine.runMaintenanceCalls);
    }

    @Test
    public void globalMaintenanceUsesGovernorAfterEachDbTick() {
        GlobalTrackingRuntimeDbEngine engine0 = new GlobalTrackingRuntimeDbEngine();
        GlobalTrackingRuntimeDbEngine engine1 = new GlobalTrackingRuntimeDbEngine();
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .engineFactory(configured -> configured.dbIndex() == 0 ? engine0 : engine1)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(128)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            new YierdisInstanceMaintenance(instance).maintenanceTick();
        }

        Assert.assertEquals(1, engine0.runMaintenanceCalls);
        Assert.assertEquals(1, engine1.runMaintenanceCalls);
        Assert.assertEquals(1, engine0.participantCleanupCalls);
        Assert.assertEquals(1, engine1.participantCleanupCalls);
    }

    @Test
    public void observabilityDbSummariesExposePerDbKeysAndExpires() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .build();
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
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

    private static final class TestSession implements yier.bubu.redis.execution.api.CommandSession {
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

        @Override
        public int respVersion() {
            return 2;
        }

        @Override
        public void setRespVersion(int respVersion) {
        }
    }

    private static final class NoopTransactionState implements TransactionState {
        @Override
        public boolean active() {
            return false;
        }

        @Override
        public boolean aborted() {
            return false;
        }

        @Override
        public void begin() {
        }

        @Override
        public void markAborted() {
        }

        @Override
        public void discard() {
        }

        @Override
        public String tryEnqueue(ExecutionRequest request) {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void forEachQueued(java.util.function.Consumer<? super ExecutionRequest> visitor) {
            java.util.Objects.requireNonNull(visitor, "visitor");
        }

        @Override
        public java.util.List<ExecutionRequest> drain() {
            return java.util.Collections.emptyList();
        }

        @Override
        public void close() {
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
        public void runMaintenance() {
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
        public void runMaintenance() {
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

    private static class TrackingRuntimeDbEngine implements RuntimeDbEngine {
        protected int bindCalls;
        protected int runMaintenanceCalls;
        protected int defragMaintenanceCalls;
        protected int shutdownCalls;

        @Override
        public void bindToCurrentThread() {
            bindCalls++;
        }

        @Override
        public void runMaintenance() {
            runMaintenanceCalls++;
        }

        @Override
        public void shutdown() {
            shutdownCalls++;
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

    private static final class DefragmentingTrackingRuntimeDbEngine
            extends TrackingRuntimeDbEngine implements DefragmentableDbEngine {
        @Override
        public void defragMaintenance() {
            defragMaintenanceCalls++;
        }
    }

    private static final class GlobalTrackingRuntimeDbEngine
            extends TrackingRuntimeDbEngine implements GlobalMaxmemoryDbEngine {
        private int participantCleanupCalls;

        @Override
        public MemoryUsageSnapshot memoryUsage() {
            return MemoryUsageSnapshot.zero();
        }

        @Override
        public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
            return MemoryReclaimResult.empty();
        }

        @Override
        public int keyCountEstimate() {
            return 0;
        }

        @Override
        public void cleanupExpired(long nowMillis) {
            participantCleanupCalls++;
        }

        @Override
        public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
            return null;
        }

        @Override
        public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
            return null;
        }

        @Override
        public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
            return false;
        }

        @Override
        public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        }
    }
}
