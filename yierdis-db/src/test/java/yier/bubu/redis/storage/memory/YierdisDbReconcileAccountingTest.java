package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.DbAccountingReconciliation;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class YierdisDbReconcileAccountingTest {
    private static final String MISCONF_DEGRADED =
            "MISCONF DB is in a degraded state; writes are disabled";

    @Test
    public void reconcileAccountingClearsDegradedRestoresWritesAndRecordsOutcome() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();
        try {
            byte[] key = b("k");
            Assert.assertTrue(db.strings().setString(key, b("old"), SetMode.NORMAL, null).value());

            PostCommitMutationException failure = Assert.assertThrows(
                    PostCommitMutationException.class,
                    () -> MutationExecutorTestSupport.replaceStringAndFailDuringRelease(
                            db, key, b("new"), "corrupt metadata")
            );
            Assert.assertTrue(failure.getCause() instanceof NativeMemoryException);
            Assert.assertTrue(db.health().degraded());
            YierdisCommandException rejected = Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> db.strings().setString(key, b("later"), SetMode.NORMAL, null)
            );
            Assert.assertEquals(MISCONF_DEGRADED, rejected.getMessage());

            long ledgerBefore = db.memoryLedger().usedBytes();
            DbAccountingReconciliation report = db.reconcileAccounting();

            long physical = db.usedBytesForMaxmemory();
            Assert.assertTrue(report.succeeded());
            Assert.assertEquals(ledgerBefore, report.ledgerUsedBeforeBytes());
            Assert.assertEquals(physical, report.physicalUsedBytes());
            Assert.assertEquals(physical - ledgerBefore, report.driftBytes());
            Assert.assertNull(report.failureTypeName());
            Assert.assertNull(report.failureMessage());
            Assert.assertTrue(report.attemptedAtMillis() > 0L);
            Assert.assertEquals(physical, db.memoryLedger().usedBytes());

            Assert.assertTrue(db.strings().setString(key, b("later"), SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(b("later"), OwnedReplyValueAssertions.stringValue(db.strings(), key));

            DbHealthSnapshot health = db.health();
            Assert.assertFalse(health.degraded());
            Assert.assertNull(health.failureTypeName());
            Assert.assertNull(health.failureMessage());
            Assert.assertEquals(0L, health.failureAtMillis());
            Assert.assertEquals(report, health.lastReconciliation());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void recoveredSnapshotClearsFailureAndNewFailureStartsFreshEpisode() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();
        try {
            byte[] key = b("k");
            Assert.assertTrue(db.strings().setString(key, b("old"), SetMode.NORMAL, null).value());
            Assert.assertThrows(
                    PostCommitMutationException.class,
                    () -> MutationExecutorTestSupport.replaceStringAndFailDuringRelease(
                            db, key, b("new"), "corrupt metadata")
            );
            Assert.assertTrue(db.health().degraded());

            db.reconcileAccounting();

            // 恢复后快照不再携带已解决事故的失败字段；事故轨迹留在 lastReconciliation。
            DbHealthSnapshot recovered = db.health();
            Assert.assertFalse(recovered.degraded());
            Assert.assertNull(recovered.failureTypeName());
            Assert.assertEquals(0L, recovered.failureAtMillis());
            Assert.assertNotNull(recovered.lastReconciliation());

            // 恢复后的新失败是一个新 episode：快照必须呈现本次失败，而不是上一次的残留。
            Assert.assertThrows(
                    PostCommitMutationException.class,
                    () -> MutationExecutorTestSupport.replaceStringAndFailDuringRelease(
                            db, key, b("later"), "second failure")
            );
            DbHealthSnapshot refailed = db.health();
            Assert.assertTrue(refailed.degraded());
            Assert.assertEquals(NativeMemoryException.class.getName(), refailed.failureTypeName());
            Assert.assertEquals("second failure", refailed.failureMessage());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void reconcileAccountingRunsOnlyOnTheOwnerThread() throws InterruptedException {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();
        try {
            AtomicReference<Throwable> rejected = new AtomicReference<>();
            Thread foreign = new Thread(() -> {
                try {
                    db.reconcileAccounting();
                } catch (Throwable failure) {
                    rejected.set(failure);
                }
            });
            foreign.start();
            foreign.join();

            Assert.assertTrue(rejected.get() instanceof IllegalStateException);
            Assert.assertEquals("YierdisDb accessed from a non-owner thread", rejected.get().getMessage());
            // thread guard 在任何对账动作之前拒绝：不产生尝试记录，健康状态不受影响。
            Assert.assertNull(db.health().lastReconciliation());
            Assert.assertFalse(db.health().degraded());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void degradedStatePersistsAcrossMaintenanceUntilExplicitReconciliation() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();
        try {
            byte[] key = b("k");
            Assert.assertTrue(db.strings().setString(key, b("old"), SetMode.NORMAL, null).value());
            Assert.assertThrows(
                    PostCommitMutationException.class,
                    () -> MutationExecutorTestSupport.replaceStringAndFailDuringRelease(
                            db, key, b("new"), "corrupt metadata")
            );
            Assert.assertTrue(db.health().degraded());

            // maintenance tick 不做自动恢复：degraded 与拒写行为与引入恢复入口前一致。
            Assert.assertThrows(YierdisCommandException.class, db::runMaintenance);
            Assert.assertTrue(db.health().degraded());
            YierdisCommandException rejected = Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> db.strings().setString(key, b("later"), SetMode.NORMAL, null)
            );
            Assert.assertEquals(MISCONF_DEGRADED, rejected.getMessage());
            Assert.assertNull(db.health().lastReconciliation());

            db.reconcileAccounting();

            Assert.assertFalse(db.health().degraded());
            Assert.assertTrue(db.strings().setString(key, b("later"), SetMode.NORMAL, null).value());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void failedReconciliationKeepsDegradedAndRecordsOutcome() {
        AtomicReference<Throwable> physicalReadFailure = new AtomicReference<>();
        YierdisDb db = TestDbSupport.openWithFactory(
                (name, maxSlots, owner) -> physicalReadFailingBackend(name, maxSlots, owner, physicalReadFailure),
                4096,
                TestDbSupport.config()
        );
        try {
            byte[] key = b("k");
            Assert.assertTrue(db.strings().setString(key, b("old"), SetMode.NORMAL, null).value());
            Assert.assertThrows(
                    PostCommitMutationException.class,
                    () -> MutationExecutorTestSupport.replaceStringAndFailDuringRelease(
                            db, key, b("new"), "corrupt metadata")
            );
            Assert.assertTrue(db.health().degraded());

            long ledgerBefore = db.memoryLedger().usedBytes();
            physicalReadFailure.set(new NativeMemoryException("physical accounting unavailable"));
            DbAccountingReconciliation failed = db.reconcileAccounting();

            Assert.assertFalse(failed.succeeded());
            Assert.assertEquals(ledgerBefore, failed.ledgerUsedBeforeBytes());
            Assert.assertEquals(-1L, failed.physicalUsedBytes());
            Assert.assertEquals(0L, failed.driftBytes());
            Assert.assertEquals(NativeMemoryException.class.getName(), failed.failureTypeName());
            Assert.assertEquals("physical accounting unavailable", failed.failureMessage());
            Assert.assertTrue(failed.attemptedAtMillis() > 0L);
            // 物理重算失败时不修正账本、不解除 degraded，失败尝试本身记入健康快照。
            Assert.assertEquals(ledgerBefore, db.memoryLedger().usedBytes());
            Assert.assertTrue(db.health().degraded());
            Assert.assertEquals(failed, db.health().lastReconciliation());
            Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> db.strings().setString(key, b("later"), SetMode.NORMAL, null)
            );

            physicalReadFailure.set(null);
            DbAccountingReconciliation recovered = db.reconcileAccounting();

            Assert.assertTrue(recovered.succeeded());
            Assert.assertFalse(db.health().degraded());
            Assert.assertEquals(recovered, db.health().lastReconciliation());
            Assert.assertTrue(db.strings().setString(key, b("later"), SetMode.NORMAL, null).value());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void physicalReadErrorPropagatesWithoutRecordingAnAttempt() {
        AtomicReference<Throwable> physicalReadFailure = new AtomicReference<>();
        YierdisDb db = TestDbSupport.openWithFactory(
                (name, maxSlots, owner) -> physicalReadFailingBackend(name, maxSlots, owner, physicalReadFailure),
                4096,
                TestDbSupport.config()
        );
        try {
            // VM 级 Error（如 OOM）不是可入账的对账失败：直接传播，不产生尝试记录。
            physicalReadFailure.set(new AssertionError("fatal physical read"));
            AssertionError fatal = Assert.assertThrows(AssertionError.class, db::reconcileAccounting);
            Assert.assertEquals("fatal physical read", fatal.getMessage());
            Assert.assertNull(db.health().lastReconciliation());
            Assert.assertFalse(db.health().degraded());
        } finally {
            physicalReadFailure.set(null);
            db.shutdown();
        }
    }

    private static StableMemoryBackend physicalReadFailingBackend(
            String name,
            int maxSlots,
            MemoryOwner owner,
            AtomicReference<Throwable> physicalReadFailure
    ) {
        StableMemoryBackend delegate = new HeapStableMemoryBackend(name, maxSlots, owner);
        return (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[]{StableMemoryBackend.class},
                (proxy, method, arguments) -> {
                    Throwable injected = physicalReadFailure.get();
                    if (injected != null && method.getName().equals("memoryUsage")) {
                        throw injected;
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                }
        );
    }
}
