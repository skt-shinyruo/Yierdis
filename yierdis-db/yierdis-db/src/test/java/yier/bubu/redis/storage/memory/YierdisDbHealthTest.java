package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.AbstractPreparedMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.YierdisHyperLogLog;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.testkit.TestBytes.slice;

public class YierdisDbHealthTest {
    private static final long TEST_UPPER_BOUND_BYTES = 1_000_000L;
    private static final int RELEASE_NATIVE_SLOT_CAPACITY = 2 * 1024 * 1024;
    private static final byte[] SUSTAINED_VALUE = new byte[256];
    private static final BytesSlice SUSTAINED_VALUE_SLICE = slice(SUSTAINED_VALUE);
    private static final String MISCONF_DEGRADED =
            "MISCONF DB is in a degraded state; writes are disabled";

    // 2000 个 member 足以让 sparse 超过 3000 字节晋升 dense（Redis hll-sparse-max-bytes 默认值）。
    private static List<byte[]> densePrefillMembers() {
        List<byte[]> members = new ArrayList<>(2000);
        for (int i = 0; i < 2000; i++) {
            members.add(b("dense-prefill:" + i));
        }
        return members;
    }

    @Test
    public void sustainedStringAndSparseHllWritesLeaveDbWritableForDenseHllPrefill() {
        YierdisDb db = TestDbSupport.openWithNativeSlotCapacity(
                0L,
                yier.bubu.redis.storage.api.MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5,
                null,
                RELEASE_NATIVE_SLOT_CAPACITY
        );
        db.bindToCurrentThread();
        try {
            for (int index = 0; index < 20_000; index++) {
                db.strings().setString(key("k", index), SUSTAINED_VALUE, SetMode.NORMAL, null);
            }
            for (int index = 0; index < 40_000; index++) {
                db.strings().setString(key("k", index % 20_000), SUSTAINED_VALUE, SetMode.NORMAL, null);
            }
            for (int index = 0; index < 40_000; index++) {
                db.strings().append(key("k", index % 20_000), SUSTAINED_VALUE_SLICE);
            }
            for (int index = 0; index < 40_000; index++) {
                db.hll().pfadd(
                        key("hll:s", index % 20_000),
                        List.of(key("member", index))
                );
            }

            Assert.assertFalse(db.health().toString(), db.health().degraded());
            byte[] sourceKey = b("hll:src");
            // Redis 语义下 PFMERGE 的 dest 只有任一参与方为 dense 才是 dense：先把 source 喂过晋升阈值。
            Assert.assertEquals(Integer.valueOf(1), db.hll().pfadd(
                    sourceKey,
                    densePrefillMembers()
            ).value());
            byte[] denseDestinationKey = b("hll:dense:0");
            db.hll().pfmerge(denseDestinationKey, List.of(sourceKey));
            EntryRecord denseDestination = db.keyLifecycle().entryRecord(denseDestinationKey);
            Assert.assertNotNull(denseDestination);
            Assert.assertTrue(YierdisHyperLogLog.isDense(
                    KeyLifecycleTestAccess.inspect(db.keyLifecycle()).stringRoot(),
                    denseDestination.valueHandle()
            ));
            Assert.assertFalse(db.health().toString(), db.health().degraded());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void pfmergeNewDestinationReservesPendingKeyDirectoryGrowth() {
        YierdisDb db = TestDbSupport.openWithNativeSlotCapacity(
                0L,
                yier.bubu.redis.storage.api.MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5,
                null,
                RELEASE_NATIVE_SLOT_CAPACITY
        );
        db.bindToCurrentThread();
        try {
            byte[] sourceKey = b("hll:merge-source");
            // 同上：source 先晋升 dense，PFMERGE 的 dest 才会写 dense。
            db.hll().pfadd(sourceKey, densePrefillMembers());

            long stagedDirectoryGrowth = 0L;
            for (int index = 0; index < 100_000; index++) {
                stagedDirectoryGrowth = KeyLifecycleTestAccess.inspect(db.keyLifecycle())
                        .keyDirectory()
                        .estimatedInsertHeapGrowthBytes();
                if (stagedDirectoryGrowth >= 1_000_000L) {
                    break;
                }
                db.strings().setString(key("directory", index), b("v"), SetMode.NORMAL, null);
            }
            Assert.assertTrue(
                    "expected the next key insertion to stage a large directory table, but growth was "
                            + stagedDirectoryGrowth,
                    stagedDirectoryGrowth >= 1_000_000L
            );

            byte[] destinationKey = b("hll:merge-destination");
            db.hll().pfmerge(destinationKey, List.of(sourceKey));

            EntryRecord destination = db.keyLifecycle().entryRecord(destinationKey);
            Assert.assertNotNull(destination);
            Assert.assertTrue(YierdisHyperLogLog.isDense(
                    KeyLifecycleTestAccess.inspect(db.keyLifecycle()).stringRoot(),
                    destination.valueHandle()
            ));
            Assert.assertFalse(db.health().toString(), db.health().degraded());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void postCommitReleaseFailureDegradesDbPreservesCommittedValueAndRejectsLaterWrites() {
        YierdisDb db = TestDbSupport.open();
        db.bindToCurrentThread();
        try {
            byte[] key = b("k");
            Assert.assertFalse(db.health().degraded());
            Assert.assertTrue(db.strings().setString(key, b("old"), SetMode.NORMAL, null).value());
            long usedBeforeReplacement = db.memoryLedger().usedBytes();

            PostCommitMutationException failure = Assert.assertThrows(
                    PostCommitMutationException.class,
                    () -> MutationExecutorTestSupport.replaceStringAndFailDuringRelease(
                            db, key, b("new"), "corrupt metadata")
            );

            Assert.assertTrue(failure.getCause() instanceof NativeMemoryException);
            DbHealthSnapshot health = db.health();
            Assert.assertTrue(health.degraded());
            Assert.assertEquals(NativeMemoryException.class.getName(), health.failureTypeName());
            Assert.assertEquals("corrupt metadata", health.failureMessage());
            Assert.assertTrue(health.failureAtMillis() > 0L);
            Assert.assertArrayEquals(b("new"), OwnedReplyValueAssertions.stringValue(db.strings(), key));
            Assert.assertEquals(usedBeforeReplacement, db.memoryLedger().usedBytes());
            Assert.assertEquals(0L, db.memoryLedger().reservedBytes());

            YierdisCommandException rejected = Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> db.strings().setString(key, b("later"), SetMode.NORMAL, null)
            );
            Assert.assertEquals(MISCONF_DEGRADED, rejected.getMessage());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void committingFailurePromotesScopeSettlesLedgerAndDoesNotAbort() {
        try (TestBackend runtime = TestBackend.open("degraded-commit")) {
            YierdisDb db = TestDbSupport.open(
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
                Assert.assertArrayEquals(b("visible"), OwnedReplyValueAssertions.stringValue(db.strings(), key));

                try (NativeAllocationScope scope = KeyLifecycleTestAccess.backend(db).beginAllocationScope()) {
                    scope.abort();
                }
            } finally {
                db.shutdown();
            }
        }
    }

    private static void publishNewStringThenFail(
            YierdisDb db,
            byte[] key,
            byte[] value,
            AtomicBoolean aborted,
            AtomicBoolean released
    ) {
        YierdisDbKeyLifecycle keyLifecycle = db.keyLifecycle();
        YierdisDbMutationExecutor executor = MutationExecutorTestSupport.create(db);
        executor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
            @Override
            public long upperBoundBytes() {
                return TEST_UPPER_BOUND_BYTES;
            }

            @Override
            public PreparedDbMutation<Void> prepare() {
                KeyLifecycleTestAccess.Inspection inspection = KeyLifecycleTestAccess.inspect(keyLifecycle);
                EntryHandle entryHandle = inspection.entryTable().reserve();
                NativeKeyDirectory.StagedInsert stagedKey = inspection.keyDirectory().stageInsert(key);
                ValueHandle valueHandle = inspection.stringRoot().store(value);
                EntryRecord record = stringRecord(keyLifecycle, stagedKey.keyHandle(), valueHandle, null);
                return new AbstractPreparedMutation<>(
                        DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                        0L
                ) {
                    @Override
                    protected Void commitPrepared() {
                        inspection.entryTable().writeReserved(entryHandle, record);
                        inspection.keyDirectory().publishStagedInsert(stagedKey, entryHandle);
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
                            inspection.entryTable().release(entryHandle);
                            keyLifecycle.releaseValue(record);
                        }
                    }
                };
            }
        });
    }

    private static EntryRecord stringRecord(
            YierdisDbKeyLifecycle keyLifecycle,
            AllocatorKeyHandle keyHandle,
            ValueHandle valueHandle,
            EntryRecord previous
    ) {
        return keyLifecycle.newRecord(
                keyHandle,
                valueHandle,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                -1L,
                previous
        );
    }

    private static byte[] key(String prefix, int index) {
        return (prefix + ':' + index).getBytes(StandardCharsets.US_ASCII);
    }

}
