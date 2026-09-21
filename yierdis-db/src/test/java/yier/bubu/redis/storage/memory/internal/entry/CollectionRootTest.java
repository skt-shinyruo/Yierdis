package yier.bubu.redis.storage.memory.internal.entry;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

/** 验证集合根只接受完整且属于当前后端的稳定句柄。 */
public class CollectionRootTest {
    @Test
    public void equalLocalRawFromAnotherBackendDoesNotAlias() {
        try (TestBackend left = TestBackend.open("collection-left");
             TestBackend right = TestBackend.open("collection-right")) {
            StableMemoryBackend leftBackend = left.backend();
            StableMemoryBackend rightBackend = right.backend();
            ListRoot roots = new ListRoot(leftBackend);
            ValueHandle local = roots.create();
            NativeHandle foreignNative = rightBackend.allocate(NativeObjectKind.LIST_ROOT, 16);
            try {
                Assert.assertEquals(local.nativeHandle().localRaw(), foreignNative.localRaw());
                Assert.assertNotEquals(local.nativeHandle().allocatorId(), foreignNative.allocatorId());
                Assert.assertFalse(roots.contains(new ValueHandle(foreignNative)));
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> roots.lpush(new ValueHandle(foreignNative), List.of(b("x")))
                );
            } finally {
                roots.release(local);
                rightBackend.free(foreignNative);
            }
        }
    }

    @Test
    public void collectionRootsRoundTripValuesAndReleaseStableHandles() {
        try (TestBackend runtime = TestBackend.open("collection-roots")) {
            StableMemoryBackend backend = runtime.backend();
            HashSeed hashSeed = HashSeed.random();
            HashTableMaintenanceRegistry maintenanceRegistry = new HashTableMaintenanceRegistry();
            HashRoot hash = new HashRoot(backend, hashSeed, maintenanceRegistry);
            SetRoot set = new SetRoot(backend, hashSeed, maintenanceRegistry);
            ZSetRoot zset = new ZSetRoot(backend, hashSeed, maintenanceRegistry);
            ListRoot list = new ListRoot(backend);
            ValueHandle hashHandle = hash.create();
            ValueHandle setHandle = set.create();
            ValueHandle zsetHandle = zset.create();
            ValueHandle listHandle = list.create();
            try {
                hash.hset(hashHandle, b("field"), b("value"));
                set.sadd(setHandle, List.of(b("member")));
                zset.zadd(zsetHandle, List.of(b("1"), b("member")));
                list.rpush(listHandle, List.of(b("a"), b("b")));
                Assert.assertArrayEquals(b("value"), hash.hget(hashHandle, b("field")));
                Assert.assertTrue(set.contains(setHandle, b("member")));
                Assert.assertEquals(1, zset.size(zsetHandle));
                List<byte[]> values = list.range(listHandle, 0, -1);
                Assert.assertEquals(2, values.size());
                Assert.assertArrayEquals(b("a"), values.get(0));
                Assert.assertArrayEquals(b("b"), values.get(1));
                Assert.assertTrue(hash.heapBytes() > 0L);
            } finally {
                hash.release(hashHandle);
                set.release(setHandle);
                zset.release(zsetHandle);
                list.release(listHandle);
            }
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void releasedRootIsStaleAndCannotBeResurrected() {
        try (TestBackend runtime = TestBackend.open("collection-stale")) {
            StableMemoryBackend backend = runtime.backend();
            ListRoot roots = new ListRoot(backend);
            ValueHandle first = roots.create();
            NativeHandle nativeHandle = first.nativeHandle();
            roots.release(first);
            Assert.assertFalse(roots.contains(first));
            ValueHandle second = roots.create();
            try {
                Assert.assertNotEquals(nativeHandle, second.nativeHandle());
                Assert.assertTrue(roots.contains(second));
            } finally {
                roots.release(second);
            }
        }
    }
}
