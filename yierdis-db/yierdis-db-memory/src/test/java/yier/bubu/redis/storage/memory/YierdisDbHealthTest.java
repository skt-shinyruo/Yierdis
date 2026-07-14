package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.AbstractPreparedMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.concurrent.atomic.AtomicBoolean;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class YierdisDbHealthTest {
    private static final long TEST_UPPER_BOUND_BYTES = 1_000_000L;
    private static final String MISCONF_DEGRADED =
            "MISCONF DB is in a degraded state; writes are disabled";

    @Test
    public void postCommitReleaseFailureDegradesDbPreservesCommittedValueAndRejectsLaterWrites() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();
        try {
            byte[] key = b("k");
            Assert.assertFalse(db.health().degraded());
            Assert.assertTrue(db.writes().strings().setString(key, b("old"), SetMode.NORMAL, null).value());
            long usedBeforeReplacement = db.memoryLedger().usedBytes();

            PostCommitMutationException failure = Assert.assertThrows(
                    PostCommitMutationException.class,
                    () -> replaceStringAndFailDuringRelease(db, key, b("new"))
            );

            Assert.assertTrue(failure.getCause() instanceof NativeMemoryException);
            DbHealthSnapshot health = db.health();
            Assert.assertTrue(health.degraded());
            Assert.assertEquals(NativeMemoryException.class.getName(), health.failureTypeName());
            Assert.assertEquals("corrupt metadata", health.failureMessage());
            Assert.assertTrue(health.failureAtMillis() > 0L);
            Assert.assertArrayEquals(b("new"), db.reads().strings().getStringBytes(key));
            Assert.assertEquals(usedBeforeReplacement, db.memoryLedger().usedBytes());
            Assert.assertEquals(0L, db.memoryLedger().reservedBytes());

            YierdisCommandException rejected = Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> db.writes().strings().setString(key, b("later"), SetMode.NORMAL, null)
            );
            Assert.assertEquals(MISCONF_DEGRADED, rejected.getMessage());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void committingFailurePromotesScopeSettlesLedgerAndDoesNotAbort() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("degraded-commit")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    0,
                    yier.bubu.redis.storage.api.MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            db.bindToCurrentThread();
            AtomicBoolean aborted = new AtomicBoolean();
            AtomicBoolean released = new AtomicBoolean();
            try {
                byte[] key = b("k");

                PostCommitMutationException failure = Assert.assertThrows(
                        PostCommitMutationException.class,
                        () -> publishNewStringThenFail(db, key, b("visible"), aborted, released)
                );

                Assert.assertTrue(failure.getCause() instanceof IllegalStateException);
                Assert.assertFalse(aborted.get());
                Assert.assertFalse(released.get());
                Assert.assertTrue(db.health().degraded());
                Assert.assertEquals(0L, db.memoryLedger().reservedBytes());
                Assert.assertEquals(DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE, db.memoryLedger().usedBytes());
                Assert.assertArrayEquals(b("visible"), db.reads().strings().getStringBytes(key));

                try (NativeAllocationScope scope = db.nativeAllocator().beginAllocationScope()) {
                    scope.abort();
                }
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static void replaceStringAndFailDuringRelease(YierdisDb db, byte[] key, byte[] nextBytes) {
        YierdisDbKeyLifecycle keyLifecycle = db.keyLifecycle();
        YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(db);
        executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
            @Override
            public long upperBoundBytes() {
                return TEST_UPPER_BOUND_BYTES;
            }

            @Override
            public PreparedDbMutation<Void> prepare() {
                EntryHandle existingEntryHandle = keyLifecycle.entryHandle(key);
                KeyHandle keyHandle = keyLifecycle.keyHandle(key);
                EntryRecord oldRecord = keyLifecycle.entryRecord(existingEntryHandle);
                ValueHandle replacement = keyLifecycle.stringRoot().store(nextBytes);
                EntryRecord newRecord = stringRecord(keyLifecycle, keyHandle, replacement, oldRecord);
                return new PreparedEntryMutation<>(
                        keyLifecycle,
                        null,
                        0L,
                        0L,
                        MutationOutcome.VALUE_CHANGED,
                        existingEntryHandle,
                        null,
                        null,
                        oldRecord,
                        newRecord,
                        true,
                        () -> {
                            keyLifecycle.releaseValue(oldRecord);
                            throw new NativeMemoryException("corrupt metadata");
                        },
                        PreparedTtlMutation.NONE
                );
            }
        });
    }

    private static void publishNewStringThenFail(
            YierdisDb db,
            byte[] key,
            byte[] value,
            AtomicBoolean aborted,
            AtomicBoolean released
    ) {
        YierdisDbKeyLifecycle keyLifecycle = db.keyLifecycle();
        YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(db);
        executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
            @Override
            public long upperBoundBytes() {
                return TEST_UPPER_BOUND_BYTES;
            }

            @Override
            public PreparedDbMutation<Void> prepare() {
                EntryHandle entryHandle = keyLifecycle.entryTable().reserve();
                NativeKeyDirectory.StagedInsert stagedKey = keyLifecycle.keyDirectory().stageInsert(key);
                ValueHandle valueHandle = keyLifecycle.stringRoot().store(value);
                EntryRecord record = stringRecord(keyLifecycle, stagedKey.keyHandle(), valueHandle, null);
                return new AbstractPreparedMutation<>(
                        DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                        0L,
                        MutationOutcome.VALUE_CHANGED
                ) {
                    @Override
                    protected Void commitPrepared() {
                        keyLifecycle.entryTable().writeReserved(entryHandle, record);
                        keyLifecycle.keyDirectory().publishStagedInsert(stagedKey, entryHandle);
                        throw new IllegalStateException("commit switch failed");
                    }

                    @Override
                    protected void releaseSupersededPrepared() {
                        released.set(true);
                    }

                    @Override
                    protected void abortPrepared() {
                        aborted.set(true);
                        try {
                            stagedKey.close();
                        } finally {
                            keyLifecycle.entryTable().release(entryHandle);
                            keyLifecycle.releaseValue(record);
                        }
                    }
                };
            }
        });
    }

    private static EntryRecord stringRecord(
            YierdisDbKeyLifecycle keyLifecycle,
            KeyHandle keyHandle,
            ValueHandle valueHandle,
            EntryRecord previous
    ) {
        return keyLifecycle.newRecord(
                keyHandle,
                valueHandle,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                -1L,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }
}
