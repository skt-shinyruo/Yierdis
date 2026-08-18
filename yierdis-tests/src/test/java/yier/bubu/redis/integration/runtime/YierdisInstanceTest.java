package yier.bubu.redis.integration.runtime;

// YierdisInstanceTest：覆盖可嵌入 instance 的装配语义与关键不变量（多 DB + global maxmemory + shared off-heap 等）。

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.integration.command.TestCommandComposition;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.embedded.TestDbRouters;
import yier.bubu.redis.runtime.embedded.YierdisInstanceObservability;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.stringValue;

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

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.runtimeAccess().bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(TestDbRouters.forInstance(instance));
            TestSession session = new TestSession();

            {
                FastTestClient client = new FastTestClient(dispatcher, session);
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

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.runtimeAccess().bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(TestDbRouters.forInstance(instance));
            {
                FastTestClient client = new FastTestClient(dispatcher);
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), key, largeValue))).value());
                long usedBefore = instance.observability().memoryStats().usedBytesForMaxmemory();
                Assert.assertTrue(usedBefore <= maxmemoryBytes);

                ReplyObject reply = client.execute(Arrays.asList(b("SET"), key, smallValue));

                Assert.assertFalse("shrinking overwrite must not be rejected by global noeviction", reply instanceof ReplyError);
                Assert.assertEquals("OK", ((ReplySimpleString) reply).value());
                Assert.assertArrayEquals(smallValue, ((ReplyBulkString) client.execute(Arrays.asList(b("GET"), key))).data());
                var after = instance.observability().memoryStats();
                Assert.assertTrue(
                        "global used bytes should shrink: before=" + usedBefore
                                + ", after=" + after.usedBytesForMaxmemory()
                                + ", nativeCommitted=" + after.nativeDataCommittedBytes()
                                + ", nativeLive=" + after.nativeDataLiveBytes()
                                + ", reclaimable=" + after.nativeReclaimableBytes(),
                        after.usedBytesForMaxmemory() < usedBefore
                );
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

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.runtimeAccess().bindToCurrentThread();

            long db0Before = instance.engines()[0].memoryStats().usedBytesForMaxmemory();

            Assert.assertTrue(instance.engines()[1].strings().setString(b("remote"), value, SetMode.NORMAL, null).value());
            long db0AfterRemoteWrite = instance.engines()[0].memoryStats().usedBytesForMaxmemory();
            Assert.assertEquals("DB1 writes must not consume DB0 maxmemory budget in per-db mode",
                    db0Before,
                    db0AfterRemoteWrite);

            Assert.assertTrue(instance.engines()[0].strings().setString(b("local"), value, SetMode.NORMAL, null).value());
            long db0AfterLocalWrite = instance.engines()[0].memoryStats().usedBytesForMaxmemory();
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
            instance.runtimeAccess().bindToCurrentThread();
            byte[] value = new byte[4_000];
            Arrays.fill(value, (byte) 'a');

            Assert.assertTrue(instance.engines()[0].strings().setString(b("a"), value, SetMode.NORMAL, null).value());
            Assert.assertTrue(instance.engines()[1].strings().setString(b("b"), value, SetMode.NORMAL, null).value());

            long off0 = instance.engines()[0].memoryStats().offHeapUsedBytes();
            long off1 = instance.engines()[1].memoryStats().offHeapUsedBytes();
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
            instance.runtimeAccess().bindToCurrentThread();
            byte[] value = new byte[4_000];
            Arrays.fill(value, (byte) 'a');

            Assert.assertTrue(instance.engines()[0].strings().setString(b("a"), value, SetMode.NORMAL, null).value());
            Assert.assertTrue(instance.engines()[1].strings().setString(b("b"), value, SetMode.NORMAL, null).value());

            long off0 = instance.engines()[0].memoryStats().offHeapUsedBytes();
            long off1 = instance.engines()[1].memoryStats().offHeapUsedBytes();

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

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.runtimeAccess().bindToCurrentThread();
            Assert.assertTrue(instance.engines()[0].strings().setString(b("k"), b("value"), SetMode.NORMAL, null).value());

            instance.runtimeAccess().maintenanceTick();

            Assert.assertArrayEquals(b("value"), stringValue(instance.engines()[0].strings(), b("k")));
            Assert.assertTrue(instance.engines()[0].memoryStats().nativeDefragLastMovedObjects() > 0L);
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

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.runtimeAccess().bindToCurrentThread();
            instance.engines()[0].strings().setString(key, initialValue, SetMode.NORMAL, null);
            instance.engines()[0].strings().setString(key, overwriteValue, SetMode.NORMAL, null);
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

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.runtimeAccess().bindToCurrentThread();
            instance.engines()[0].strings().setString(b("k0"), value, SetMode.NORMAL, null);
            instance.engines()[1].strings().setString(b("k1"), value, SetMode.NORMAL, null);
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

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.runtimeAccess().bindToCurrentThread();
            instance.engines()[dbIndex].strings().setString(key, value, SetMode.NORMAL, null);
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

    @Test
    public void unboundOrCrossThreadAccessFailsFast() throws Exception {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder().build();
        try (YierdisInstance instance = YierdisInstance.create(config)) {
            try {
                instance.engines()[0].memoryStats();
                Assert.fail("expected fail-fast before bind");
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("bindToCurrentThread"));
            }

            instance.runtimeAccess().bindToCurrentThread();

            AtomicReference<Throwable> errRef = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try {
                    instance.engines()[0].memoryStats();
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
    public void observabilityDbSummariesExposePerDbKeysAndExpires() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.PER_DB)
                .build();
        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.runtimeAccess().bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(TestDbRouters.forInstance(instance));
            TestSession session = new TestSession();
            {
                FastTestClient client = new FastTestClient(dispatcher, session);
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
        public String clientName() {
            return clientName;
        }

        @Override
        public void setClientName(String clientName) {
            this.clientName = clientName;
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

    }

}
