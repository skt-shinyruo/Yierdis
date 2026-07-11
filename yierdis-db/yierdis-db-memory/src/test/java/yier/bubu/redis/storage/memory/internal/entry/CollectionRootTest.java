package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.YierdisValue;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CollectionRootTest {
    @Test
    public void collectionRootTableDoesNotOwnBoxedHandleMapsOrSets() {
        Assert.assertFalse(Arrays.stream(NativeCollectionRootTable.class.getDeclaredFields())
                .anyMatch(field -> Map.class.isAssignableFrom(field.getType())
                        || Set.class.isAssignableFrom(field.getType())));
    }

    @Test
    public void hashSetAndZsetRootsRoundTripMembers() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             HashRoot hash = new HashRoot(allocator);
             SetRoot set = new SetRoot(allocator);
             ZSetRoot zset = new ZSetRoot(allocator)) {
            ValueHandle hashHandle = hash.create();
            ValueHandle setHandle = set.create();
            ValueHandle zsetHandle = zset.create();

            hash.hset(hashHandle, b("field"), b("value"));
            set.sadd(setHandle, List.of(b("alpha"), b("beta")));
            zset.zadd(zsetHandle, List.of(b("1"), b("m1"), b("2"), b("m2")));

            Assert.assertArrayEquals(b("value"), hash.hget(hashHandle, b("field")));
            Assert.assertEquals(2, set.size(setHandle));
            Assert.assertEquals(2, zset.size(zsetHandle));
        }
    }

    @Test
    public void collectionRootHandlesAreAllocatorBackedAndStaleAfterRelease() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root-native-handles");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
             ListRoot list = new ListRoot(allocator);
             HashRoot hash = new HashRoot(allocator);
             SetRoot set = new SetRoot(allocator);
             ZSetRoot zset = new ZSetRoot(allocator)) {
            assertReleasedHandleIsAllocatorStale(allocator, NativeObjectKind.LIST_ROOT, list.create(), list::contains, list::release);
            assertReleasedHandleIsAllocatorStale(allocator, NativeObjectKind.HASH_ROOT, hash.create(), hash::contains, hash::release);
            assertReleasedHandleIsAllocatorStale(allocator, NativeObjectKind.SET_ROOT, set.create(), set::contains, set::release);
            assertReleasedHandleIsAllocatorStale(allocator, NativeObjectKind.ZSET_ROOT, zset.create(), zset::contains, zset::release);
        }
    }

    @Test
    public void collectionRootAdapterTablesDoNotResurrectExternallyFreedHandles() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root-native-liveness");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
             ListRoot list = new ListRoot(allocator);
             HashRoot hash = new HashRoot(allocator);
             SetRoot set = new SetRoot(allocator);
             ZSetRoot zset = new ZSetRoot(allocator)) {
            assertAllocatorRemainsLivenessAuthority(allocator, NativeObjectKind.LIST_ROOT, list.create(), list::contains, list::size);
            assertAllocatorRemainsLivenessAuthority(allocator, NativeObjectKind.HASH_ROOT, hash.create(), hash::contains, hash::size);
            assertAllocatorRemainsLivenessAuthority(allocator, NativeObjectKind.SET_ROOT, set.create(), set::contains, set::size);
            assertAllocatorRemainsLivenessAuthority(allocator, NativeObjectKind.ZSET_ROOT, zset.create(), zset::contains, zset::size);
        }
    }

    @Test
    public void hashSetAndZsetAdaptersCrossNativeObjectTableSegments() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root-segments");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 8192);
             HashRoot hash = new HashRoot(allocator);
             SetRoot set = new SetRoot(allocator);
             ZSetRoot zset = new ZSetRoot(allocator)) {
            NativeHandle[] fillers = new NativeHandle[4095];
            for (int i = 0; i < fillers.length; i++) {
                fillers[i] = allocator.allocate(NativeObjectKind.GENERIC, 1);
            }

            ValueHandle hashHandle = hash.create();
            ValueHandle setHandle = set.create();
            ValueHandle zsetHandle = zset.create();

            Assert.assertEquals(4096L, hashHandle.nativeHandle().slotId());
            Assert.assertEquals(4097L, setHandle.nativeHandle().slotId());
            Assert.assertEquals(4098L, zsetHandle.nativeHandle().slotId());
            Assert.assertTrue(hash.contains(hashHandle));
            Assert.assertTrue(set.contains(setHandle));
            Assert.assertTrue(zset.contains(zsetHandle));

            hash.release(hashHandle);
            set.release(setHandle);
            zset.release(zsetHandle);
            for (NativeHandle filler : fillers) {
                allocator.free(filler);
            }
        }
    }

    @Test
    public void reusedAdapterSlotDoesNotReviveStaleGeneration() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root-adapter-generation");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32)) {
            NativeCollectionRootTable<CountingListValue> table = new NativeCollectionRootTable<>(
                    allocator,
                    NativeObjectKind.LIST_ROOT,
                    "list",
                    false
            );
            ValueHandle first = table.create(ignored -> new CountingListValue());
            table.release(first);
            ValueHandle second = table.create(ignored -> new CountingListValue());

            Assert.assertEquals(first.nativeHandle().slotId(), second.nativeHandle().slotId());
            Assert.assertNotEquals(first.raw(), second.raw());
            Assert.assertFalse(table.contains(first));
            try {
                table.require(first);
                Assert.fail("expected stale native handle");
            } catch (StaleNativeHandleException expected) {
                // expected
            }
            Assert.assertTrue(table.contains(second));
            table.release(second);
        }
    }

    @Test
    public void clearClosesEachAdapterOnceAndReleasesEmptyDirectorySegments() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root-adapter-clear");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 8192)) {
            NativeCollectionRootTable<CountingListValue> table = new NativeCollectionRootTable<>(
                    allocator,
                    NativeObjectKind.LIST_ROOT,
                    "list",
                    false
            );
            CountingListValue[] values = new CountingListValue[4097];
            for (int i = 0; i < values.length; i++) {
                values[i] = new CountingListValue();
                CountingListValue value = values[i];
                table.create(ignored -> value);
            }

            Assert.assertEquals(2, table.adapterSegmentCount());
            table.clear();

            for (CountingListValue value : values) {
                Assert.assertEquals(1, value.closeCalls());
            }
            Assert.assertEquals(0, table.adapterSegmentCount());
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
        }
    }

    @Test
    public void collectionRootReleaseKeepsAdapterRetryableButFreesRootWhenAdapterCloseFails() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root-release-retry");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32)) {
            NativeCollectionRootTable<ThrowOnceListValue> table = new NativeCollectionRootTable<>(
                    allocator,
                    NativeObjectKind.LIST_ROOT,
                    "list",
                    false
            );
            ThrowOnceListValue value = new ThrowOnceListValue();
            ValueHandle handle = table.create(rootHandle -> value);

            try {
                table.release(handle);
                Assert.fail("expected injected close failure");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("injected close failure"));
            }

            Assert.assertFalse(table.contains(handle));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(1, value.closeCalls());
            assertStale(allocator, handle.nativeHandle());

            table.release(handle);

            Assert.assertFalse(table.contains(handle));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(2, value.closeCalls());
            assertStale(allocator, handle.nativeHandle());
        }
    }

    @Test
    public void collectionRootReleaseKeepsAdapterRetryableWhenRootNativeFreeFails() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root-release-free-retry");
             FailOnceFreeAllocator allocator = new FailOnceFreeAllocator(runtime, 32)) {
            NativeCollectionRootTable<CountingListValue> table = new NativeCollectionRootTable<>(
                    allocator,
                    NativeObjectKind.LIST_ROOT,
                    "list",
                    false
            );
            CountingListValue value = new CountingListValue();
            ValueHandle handle = table.create(rootHandle -> {
                allocator.failNextFree(rootHandle);
                return value;
            });

            try {
                table.release(handle);
                Assert.fail("expected injected root free failure");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("injected root free failure"));
            }

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(1, value.closeCalls());

            table.release(handle);

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(2, value.closeCalls());
            assertStale(allocator, handle.nativeHandle());
        }
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static void assertReleasedHandleIsAllocatorStale(
            NativeAllocator allocator,
            NativeObjectKind kind,
            ValueHandle handle,
            RootContains contains,
            RootRelease release
    ) {
        assertNativeKind(handle, kind);
        Assert.assertTrue(contains.contains(handle));
        Assert.assertEquals(1L, allocator.stats().objectCount(kind));

        release.release(handle);

        Assert.assertFalse(contains.contains(handle));
        Assert.assertEquals(0L, allocator.stats().objectCount(kind));
        assertStale(allocator, handle.nativeHandle());
    }

    private static void assertAllocatorRemainsLivenessAuthority(
            NativeAllocator allocator,
            NativeObjectKind kind,
            ValueHandle handle,
            RootContains contains,
            RootSize size
    ) {
        assertNativeKind(handle, kind);
        Assert.assertTrue(contains.contains(handle));

        allocator.free(handle.nativeHandle());

        Assert.assertEquals(0L, allocator.stats().objectCount(kind));
        Assert.assertFalse("adapter table must not resurrect " + kind, contains.contains(handle));
        try {
            size.size(handle);
            Assert.fail("expected stale native handle");
        } catch (StaleNativeHandleException expected) {
            // expected
        }
    }

    private static void assertNativeKind(ValueHandle handle, NativeObjectKind kind) {
        NativeHandle nativeHandle = handle.nativeHandle();
        Assert.assertEquals(kind.domain(), nativeHandle.domain());
        Assert.assertEquals(kind.code(), nativeHandle.kindCode());
    }

    private static void assertStale(NativeAllocator allocator, NativeHandle handle) {
        try {
            allocator.resolve(handle, NativeAccessMode.READ_ONLY).close();
            Assert.fail("expected stale native handle");
        } catch (StaleNativeHandleException expected) {
            // expected
        }
    }

    @FunctionalInterface
    private interface RootContains {
        boolean contains(ValueHandle handle);
    }

    @FunctionalInterface
    private interface RootRelease {
        void release(ValueHandle handle);
    }

    @FunctionalInterface
    private interface RootSize {
        int size(ValueHandle handle);
    }

    private static final class FailOnceFreeAllocator implements NativeAllocator {
        private final NativeAllocator delegate;
        private long failedRawHandle;
        private boolean armed;

        private FailOnceFreeAllocator(YierdisFfmMemoryRuntime runtime, int maxSlots) {
            this.delegate = new YierdisStableNativeAllocator(runtime, maxSlots);
        }

        private void failNextFree(NativeHandle handle) {
            failedRawHandle = handle.raw();
            armed = true;
        }

        @Override
        public NativeHandle allocate(NativeObjectKind kind, int size) {
            return delegate.allocate(kind, size);
        }

        @Override
        public NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
            return delegate.realloc(handle, newSize, policy);
        }

        @Override
        public void free(NativeHandle handle) {
            if (armed && handle.raw() == failedRawHandle) {
                armed = false;
                throw new IllegalStateException("injected root free failure");
            }
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

    private static final class CountingListValue implements YierdisValue {
        private int closeCalls;

        @Override
        public ValueType type() {
            return ValueType.LIST;
        }

        @Override
        public ValueEncoding encoding() {
            return ValueEncoding.LIST_PACKED;
        }

        @Override
        public void close() {
            closeCalls++;
        }

        int closeCalls() {
            return closeCalls;
        }
    }

    private static final class ThrowOnceListValue implements YierdisValue {
        private boolean fail = true;
        private int closeCalls;

        @Override
        public ValueType type() {
            return ValueType.LIST;
        }

        @Override
        public ValueEncoding encoding() {
            return ValueEncoding.LIST_PACKED;
        }

        @Override
        public void close() {
            closeCalls++;
            if (fail) {
                fail = false;
                throw new IllegalStateException("injected close failure");
            }
        }

        int closeCalls() {
            return closeCalls;
        }
    }
}
