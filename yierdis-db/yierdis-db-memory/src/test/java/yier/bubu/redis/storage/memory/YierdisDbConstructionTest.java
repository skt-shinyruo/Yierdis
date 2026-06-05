package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.DbChange;
import yier.bubu.redis.storage.api.DbChangeContext;
import yier.bubu.redis.storage.api.DbChangeKind;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.Collections;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class YierdisDbConstructionTest {
    @Test
    public void nullAndBlankMaxmemoryPoliciesDefaultToNoeviction() {
        assertConstructsWithPolicy(null);
        assertConstructsWithPolicy("");
        assertConstructsWithPolicy("   ");
    }

    @Test
    public void typedConfigDefaultsNullPolicyToNoeviction() {
        YierdisDbConfig config = YierdisDbConfig.create(0, null, 5, 5, 5);
        Assert.assertSame(MaxmemoryPolicy.NOEVICTION, config.maxmemoryPolicy);
    }

    @Test
    public void policyParsingNormalizesCaseAndUnderscore() {
        assertConstructsWithPolicy("ALLKEYS_RANDOM");
        assertConstructsWithPolicy("allkeys_LRU");
        assertConstructsWithPolicy("  NoEviction  ");
    }

    @Test
    public void typedConfigComputesLruEnabledFromCorePolicy() {
        YierdisDbConfig lru = YierdisDbConfig.create(1, MaxmemoryPolicy.ALLKEYS_LRU, 5, 5, 5);
        Assert.assertTrue(lru.lruEnabled);

        YierdisDbConfig noLimit = YierdisDbConfig.create(0, MaxmemoryPolicy.ALLKEYS_LRU, 5, 5, 5);
        Assert.assertFalse(noLimit.lruEnabled);

        YierdisDbConfig random = YierdisDbConfig.create(1, MaxmemoryPolicy.ALLKEYS_RANDOM, 5, 5, 5);
        Assert.assertFalse(random.lruEnabled);
    }

    @Test
    public void defaultDbCreatesNativeRuntimeAndStableNativeStorage() {
        YierdisDb db = new YierdisDb();
        try {
            Assert.assertNotNull(db.keyLifecycle().nativeAllocator());
            Assert.assertNotNull(db.keyLifecycle().memoryRuntime());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void sharedRuntimeDbUsesProvidedRuntime() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("shared-runtime-construction")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            try {
                Assert.assertSame(runtime, db.keyLifecycle().memoryRuntime());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void unknownPolicyStillThrowsIllegalArgumentException() {
        try {
            YierdisDb.createWithOwnedFfmRuntime(0, "unknown-policy", 5, 5, 5);
            Assert.fail("unknown policy should fail construction");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("unknown maxmemory policy"));
        }
    }

    @Test
    public void invalidConstructionNumbersStillThrowIllegalArgumentException() {
        assertInvalid(-1, "noeviction", 5, 5, 5, "maxmemoryBytes");
        assertInvalid(0, "noeviction", 0, 5, 5, "maxmemorySamples");
        assertInvalid(0, "noeviction", 5, 0, 5, "evictionTimeLimitMillis");
        assertInvalid(0, "noeviction", 5, 5, 0, "expireCleanupTimeLimitMillis");
    }

    @Test
    public void storageComponentsCarryNativeEntryDirectoryGraph() {
        YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(null, false);
        Assert.assertNotNull(storage.entries);
        Assert.assertNotNull(storage.keyDirectory);
        storage.resources.releaseAll(
                storage.expires,
                storage.entries,
                storage.keyDirectory,
                storage.stringRoot,
                storage.listRoot,
                storage.hashRoot,
                storage.setRoot,
                storage.zsetRoot
        );
    }

    @Test
    public void storageComponentsShareOneNativeAllocatorForEntriesStringsAndCollectionRoots() {
        YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(null, false);
        try {
            Assert.assertNotNull(storage.nativeAllocator);
            Assert.assertSame(storage.nativeAllocator, nativeAllocator(storage.entries));
            Assert.assertSame(storage.nativeAllocator, nativeAllocator(storage.stringRoot));
            Assert.assertSame(storage.nativeAllocator, nativeAllocator(storage.listRoot));
            Assert.assertSame(storage.nativeAllocator, nativeAllocator(storage.hashRoot));
            Assert.assertSame(storage.nativeAllocator, nativeAllocator(storage.setRoot));
            Assert.assertSame(storage.nativeAllocator, nativeAllocator(storage.zsetRoot));

            storage.stringRoot.store(bytes("value"));
            storage.entries.allocate(new EntryRecord(
                    1L,
                    valueHandle(2L),
                    3,
                    ValueType.STRING,
                    ValueEncoding.STRING_RAW,
                    0,
                    -1L,
                    0L,
                    0L
            ));

            Assert.assertTrue(storage.nativeAllocator.stats().objectCount(NativeObjectKind.ENTRY_RECORD) > 0L);
            Assert.assertTrue(storage.nativeAllocator.stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);
        } finally {
            storage.resources.releaseAll(
                    storage.expires,
                    storage.entries,
                    storage.keyDirectory,
                    storage.stringRoot,
                    storage.listRoot,
                    storage.hashRoot,
                    storage.setRoot,
                    storage.zsetRoot
            );
        }
    }

    @Test
    public void storageComponentsReserveNativeSlotsForEntriesStringsKeysAndCollectionRoots() {
        Assert.assertEquals(256 * 1024, YierdisDbStorageComponents.sharedNativeSlotCapacity());
    }

    @Test
    public void computeWithHandleReleasesNewStringWhenEntryAllocationFails() {
        YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-compute-entry-allocation-failure");
        NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1);
        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(
                runtime,
                allocator,
                true,
                true
        );
        YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator);
        EntryTable entries = new EntryTable(runtime, allocator);
        NativeKeyDirectory keyDirectory = new NativeKeyDirectory(allocator);
        StringRoot stringRoot = new StringRoot(allocator);
        ListRoot listRoot = new ListRoot(runtime);
        HashRoot hashRoot = new HashRoot(runtime);
        SetRoot setRoot = new SetRoot(runtime);
        ZSetRoot zsetRoot = new ZSetRoot(runtime);
        try {
            YierdisDbKeyLifecycle lifecycle = new YierdisDbKeyLifecycle(
                    expires,
                    allocator,
                    runtime,
                    entries,
                    keyDirectory,
                    stringRoot,
                    listRoot,
                    hashRoot,
                    setRoot,
                    zsetRoot,
                    () -> 1L,
                    ignored -> {
                    }
            );

            try {
                lifecycle.computeWithHandleResult(bytes("entry-allocation-fails"), (keyHandle, oldRecord) -> {
                    Assert.assertNull(oldRecord);
                    ValueHandle valueHandle = stringRoot.store(bytes("value"));
                    EntryRecord next = lifecycle.newRecord(
                            keyHandle,
                            valueHandle,
                            ValueType.STRING,
                            ValueEncoding.STRING_RAW,
                            -1L,
                            0L,
                            null
                    );
                    return YierdisDbKeyLifecycle.EntryMutationResult.of(next, null);
                });
                Assert.fail("entry allocation should fail when the shared native allocator has no free slots");
            } catch (NativeMemoryException e) {
                Assert.assertTrue(e.getMessage().contains("native object slot limit exceeded"));
            }

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.STRING_BYTES));
        } finally {
            resources.releaseAll(
                    expires,
                    entries,
                    keyDirectory,
                    stringRoot,
                    listRoot,
                    hashRoot,
                    setRoot,
                    zsetRoot
            );
        }
    }

    @Test
    public void computeWithHandleResultStoresRecordAndReturnsMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();

            String result = lifecycle.computeWithHandleResult(bytes("metadata-key"), (keyHandle, oldRecord) -> {
                Assert.assertNull(oldRecord);
                ValueHandle valueHandle = lifecycle.stringRoot().store(bytes("metadata-value"));
                EntryRecord next = lifecycle.newRecord(
                        keyHandle,
                        valueHandle,
                        ValueType.STRING,
                        ValueEncoding.STRING_RAW,
                        -1L,
                        123L,
                        null
                );
                return new YierdisDbKeyLifecycle.EntryMutationResult<>(next, "created:" + keyHandle.dictHash());
            });

            Assert.assertTrue(result.startsWith("created:"));
            EntryRecord stored = lifecycle.entryRecord(bytes("metadata-key"));
            Assert.assertNotNull(stored);
            Assert.assertEquals(ValueType.STRING, stored.type());
            Assert.assertArrayEquals(bytes("metadata-value"), lifecycle.stringRoot().copy(stored.valueHandle()));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void setBitReleasesNewStringWhenEnsureLengthFails() {
        YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-setbit-ensure-length-failure");
        NativeAllocator allocator = new ReallocSlotLimitNativeAllocator(new YierdisStableNativeAllocator(runtime, 1));
        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(
                runtime,
                allocator,
                true,
                true
        );
        YierdisFfmExpireIndex expires = new YierdisFfmExpireIndex(runtime, allocator);
        EntryTable entries = new EntryTable(runtime, allocator);
        NativeKeyDirectory keyDirectory = new NativeKeyDirectory(allocator);
        StringRoot stringRoot = new StringRoot(allocator);
        ListRoot listRoot = new ListRoot(runtime);
        HashRoot hashRoot = new HashRoot(runtime);
        SetRoot setRoot = new SetRoot(runtime);
        ZSetRoot zsetRoot = new ZSetRoot(runtime);
        try {
            YierdisDbKeyLifecycle lifecycle = new YierdisDbKeyLifecycle(
                    expires,
                    allocator,
                    runtime,
                    entries,
                    keyDirectory,
                    stringRoot,
                    listRoot,
                    hashRoot,
                    setRoot,
                    zsetRoot,
                    () -> 1L,
                    ignored -> {
                    }
            );
            InMemoryLedger ledger = new InMemoryLedger(0);
            YierdisDbMutationExecutor executor = new YierdisDbMutationExecutor(() -> {
            }, ledger);
            YierdisStringOps strings = new YierdisStringOps(new YierdisDbInternals() {
                @Override
                public void checkThread() {
                }

                @Override
                public <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan) {
                    return executor.execute(plan);
                }

                @Override
                public YierdisDbKeyLifecycle keyLifecycle() {
                    return lifecycle;
                }

                @Override
                public MemoryLedger ledger() {
                    return ledger;
                }
            });
            byte[] key = bytes("setbit-realloc-fails");

            try {
                strings.setBit(key, 128L, 1);
                Assert.fail("setbit should fail when string growth cannot realloc");
            } catch (NativeMemoryException e) {
                Assert.assertTrue(e.getMessage().contains("native object slot limit exceeded"));
            }

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.STRING_BYTES));
            Assert.assertNull(lifecycle.entryHandle(key));
            Assert.assertNull(lifecycle.entryRecord(key));
        } finally {
            resources.releaseAll(
                    expires,
                    entries,
                    keyDirectory,
                    stringRoot,
                    listRoot,
                    hashRoot,
                    setRoot,
                    zsetRoot
            );
        }
    }

    @Test
    public void shutdownReleasesNativeEntryDirectoryResources() {
        YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-entry-graph-test");
        YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        db.bindToCurrentThread();
        EntryHandle handle = db.keyLifecycle().entryTable().allocate(new EntryRecord(
                1L,
                valueHandle(2L),
                3,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                0,
                -1L,
                0L,
                0L
        ));
        db.keyLifecycle().keyDirectory().compute("native-key".getBytes(StandardCharsets.UTF_8), (key, old) -> handle);
        Assert.assertTrue(runtime.usedBytes() > 0L);

        db.shutdown();

        Assert.assertEquals(0L, runtime.usedBytes());
        runtime.close();
    }

    @Test
    public void normalWritesUpdateNativeEntryDirectoryMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-meta");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);

            EntryHandle handle = db.keyLifecycle().entryHandle(key);
            Assert.assertNotNull(handle);
            EntryRecord record = db.keyLifecycle().entryRecord(handle);
            Assert.assertNotNull(record);
            Assert.assertEquals(ValueType.STRING, record.type());
            Assert.assertEquals(ValueEncoding.STRING_EMBSTR, record.encoding());
            Assert.assertEquals(-1L, record.expireAtMillis());
            Assert.assertTrue(record.version() > 0L);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void ttlAndDeleteUpdateNativeEntryDirectoryMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-ttl");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().pexpire(view(key), 60_000L).value());

            EntryRecord ttlRecord = db.keyLifecycle().entryRecord(key);
            Assert.assertNotNull(ttlRecord);
            Assert.assertTrue(ttlRecord.expireAtMillis() > System.currentTimeMillis());

            Assert.assertEquals(1L, (long) db.writes().keyspace().del(Collections.singletonList(key)).value());
            Assert.assertNull(db.keyLifecycle().entryHandle(key));
            Assert.assertNull(db.keyLifecycle().entryRecord(key));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void activeExpirationCleanupUnlinksNativeEntryMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-expire-cleanup");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);
            Assert.assertNotNull(db.keyLifecycle().entryHandle(key));
            Assert.assertEquals(1, db.keyLifecycle().entryTable().size());
            Assert.assertTrue(db.writes().ttl().pexpire(view(key), 1L).value());

            db.cleanupExpired(System.currentTimeMillis() + 1_000L);

            Assert.assertNull(db.keyLifecycle().entryHandle(key));
            Assert.assertEquals(0, db.keyLifecycle().entryTable().size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void evictionUnlinksNativeEntryMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-evict");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);
            Assert.assertNotNull(db.keyLifecycle().entryHandle(key));
            Assert.assertEquals(1, db.keyLifecycle().entryTable().size());

            MaxmemoryCandidate candidate = db.sampleCandidate(MaxmemoryPolicy.ALLKEYS_RANDOM, System.currentTimeMillis());
            Assert.assertNotNull(candidate);
            Assert.assertSame(db, candidate.owner());
            Assert.assertTrue(db.evict(candidate, System.currentTimeMillis()));

            Assert.assertNull(db.keyLifecycle().entryHandle(key));
            Assert.assertEquals(0, db.keyLifecycle().entryTable().size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void evictionEmitsSyntheticDeleteChange() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-evict-event");
            List<DbChange> changes = new ArrayList<>();

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);
            MaxmemoryCandidate candidate = db.sampleCandidate(MaxmemoryPolicy.ALLKEYS_RANDOM, System.currentTimeMillis());
            Assert.assertNotNull(candidate);

            try (DbChangeContext.Scope ignored = DbChangeContext.open(changes::add)) {
                Assert.assertTrue(db.evict(candidate, System.currentTimeMillis()));
            }

            assertSyntheticDelete(changes, "native-evict-event", DbChangeKind.EVICTED);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void scannedMaxmemoryCandidateIsOwnedByPublicDbAndEvictableByIt() {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(1024 * 1024, MaxmemoryPolicy.ALLKEYS_LRU, 5, 5, 5);
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("public-scan-owner");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);
            MaxmemoryCandidate candidate = db.scanBestCandidate(MaxmemoryPolicy.ALLKEYS_LRU, System.currentTimeMillis());

            Assert.assertNotNull(candidate);
            Assert.assertSame(db, candidate.owner());
            Assert.assertTrue(db.evict(candidate, System.currentTimeMillis()));
            Assert.assertNull(db.keyLifecycle().entryHandle(key));
        } finally {
            db.shutdown();
        }
    }

    private static void assertConstructsWithPolicy(String policy) {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(0, policy, 5, 5, 5);
        try {
            db.bindToCurrentThread();
        } finally {
            db.shutdown();
        }
    }

    private static void assertInvalid(
            long maxmemoryBytes,
            String policy,
            int samples,
            long evictionMillis,
            long expireMillis,
            String messagePart
    ) {
        try {
            YierdisDb.createWithOwnedFfmRuntime(maxmemoryBytes, policy, samples, evictionMillis, expireMillis);
            Assert.fail("invalid construction should fail: " + messagePart);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains(messagePart));
        }
    }

    private static void assertSyntheticDelete(List<DbChange> changes, String key, DbChangeKind kind) {
        Assert.assertEquals(1, changes.size());
        DbChange change = changes.get(0);
        Assert.assertEquals(kind, change.kind());
        Assert.assertEquals(0, change.dbIndex());
        Assert.assertEquals("DEL", changeArg(change, 0));
        Assert.assertEquals(key, changeArg(change, 1));
    }

    private static String changeArg(DbChange change, int index) {
        return new String(change.commandArgv()[index], StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ValueHandle valueHandle(long slotId) {
        NativeObjectKind kind = NativeObjectKind.STRING_BYTES;
        return ValueHandle.fromNativeHandle(NativeHandle.of(kind.domain(), kind, slotId, 1, 0));
    }

    private static NativeAllocator nativeAllocator(Object owner) {
        try {
            java.lang.reflect.Method allocator = owner.getClass().getDeclaredMethod("allocator");
            allocator.setAccessible(true);
            return (NativeAllocator) allocator.invoke(owner);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("allocator accessor should be available", e);
        }
    }

    private static yier.bubu.redis.bytes.BytesView view(byte[] data) {
        return new yier.bubu.redis.bytes.BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                if (index < 0 || index >= data.length) {
                    throw new IndexOutOfBoundsException();
                }
                return data[index];
            }
        };
    }

    private static final class ReallocSlotLimitNativeAllocator implements NativeAllocator {
        private final NativeAllocator delegate;

        private ReallocSlotLimitNativeAllocator(NativeAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public NativeHandle allocate(NativeObjectKind kind, int size) {
            return delegate.allocate(kind, size);
        }

        @Override
        public NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
            NativeHandle unexpected = delegate.allocate(NativeObjectKind.STRING_BYTES, newSize);
            delegate.free(unexpected);
            throw new AssertionError("expected one-slot native allocator to reject realloc growth");
        }

        @Override
        public void free(NativeHandle handle) {
            delegate.free(handle);
        }

        @Override
        public void pin(NativeHandle handle) {
            delegate.pin(handle);
        }

        @Override
        public void unpin(NativeHandle handle) {
            delegate.unpin(handle);
        }

        @Override
        public NativeEpochScope beginEpoch(NativeEpochKind kind) {
            return delegate.beginEpoch(kind);
        }

        @Override
        public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
            return delegate.resolve(handle, mode);
        }

        @Override
        public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
            return delegate.defragOne(handle, maxMoveBytes);
        }

        @Override
        public NativeDefragReport defragCycle(NativeDefragOptions options) {
            return delegate.defragCycle(options);
        }

        @Override
        public NativeAllocatorStats stats() {
            return delegate.stats();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
