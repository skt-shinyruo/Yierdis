package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CollectionRootTest {
    @Test
    public void hashSetAndZsetRootsRoundTripMembers() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root");
             HashRoot hash = new HashRoot(runtime);
             SetRoot set = new SetRoot(runtime);
             ZSetRoot zset = new ZSetRoot(runtime)) {
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
             ListRoot list = new ListRoot(runtime, allocator);
             HashRoot hash = new HashRoot(runtime, allocator);
             SetRoot set = new SetRoot(runtime, allocator);
             ZSetRoot zset = new ZSetRoot(runtime, allocator)) {
            assertReleasedHandleIsAllocatorStale(allocator, NativeObjectKind.LIST_NODE, list.create(), list::contains, list::release);
            assertReleasedHandleIsAllocatorStale(allocator, NativeObjectKind.HASH_NODE, hash.create(), hash::contains, hash::release);
            assertReleasedHandleIsAllocatorStale(allocator, NativeObjectKind.SET_NODE, set.create(), set::contains, set::release);
            assertReleasedHandleIsAllocatorStale(allocator, NativeObjectKind.ZSET_NODE, zset.create(), zset::contains, zset::release);
        }
    }

    @Test
    public void collectionRootAdapterTablesDoNotResurrectExternallyFreedHandles() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root-native-liveness");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
             ListRoot list = new ListRoot(runtime, allocator);
             HashRoot hash = new HashRoot(runtime, allocator);
             SetRoot set = new SetRoot(runtime, allocator);
             ZSetRoot zset = new ZSetRoot(runtime, allocator)) {
            assertAllocatorRemainsLivenessAuthority(allocator, NativeObjectKind.LIST_NODE, list.create(), list::contains, list::size);
            assertAllocatorRemainsLivenessAuthority(allocator, NativeObjectKind.HASH_NODE, hash.create(), hash::contains, hash::size);
            assertAllocatorRemainsLivenessAuthority(allocator, NativeObjectKind.SET_NODE, set.create(), set::contains, set::size);
            assertAllocatorRemainsLivenessAuthority(allocator, NativeObjectKind.ZSET_NODE, zset.create(), zset::contains, zset::size);
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
}
