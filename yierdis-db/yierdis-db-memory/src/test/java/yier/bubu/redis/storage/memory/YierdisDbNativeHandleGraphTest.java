package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class YierdisDbNativeHandleGraphTest {
    @Test
    public void visitorEnumeratesLiveKeysEntriesAndValueRoots() {
        YierdisDb db = TestDbSupport.open(
                0,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5
        );
        try {
            db.bindToCurrentThread();
            Assert.assertTrue(db.writes().strings().setString(b("string"), b("value"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("list"), List.of(b("a"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("hash"), List.of(b("field"), b("value"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("member"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("zset"), List.of(b("1"), b("member"))).value());

            EnumMap<YierdisDbNativeHandleGraph.Role, Integer> roleCounts =
                    new EnumMap<>(YierdisDbNativeHandleGraph.Role.class);
            YierdisDbNativeHandleGraph.visitReachable(db.keyLifecycle(), (role, handle, record) -> {
                roleCounts.merge(role, 1, Integer::sum);
                Assert.assertEquals(KeyLifecycleTestAccess.backend(db).allocatorId(), handle.allocatorId());
                Assert.assertNotNull(record);
            });

            Assert.assertEquals(Integer.valueOf(5), roleCounts.get(YierdisDbNativeHandleGraph.Role.KEY_BYTES));
            Assert.assertEquals(Integer.valueOf(5), roleCounts.get(YierdisDbNativeHandleGraph.Role.ENTRY_RECORD));
            Assert.assertEquals(Integer.valueOf(1), roleCounts.get(YierdisDbNativeHandleGraph.Role.STRING_VALUE));
            Assert.assertEquals(Integer.valueOf(4), roleCounts.get(YierdisDbNativeHandleGraph.Role.COLLECTION_ROOT));
            Assert.assertEquals(Integer.valueOf(4), roleCounts.get(YierdisDbNativeHandleGraph.Role.COLLECTION_INTERNAL));

            Assert.assertEquals(5L, KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(5L, KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(1L, KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.STRING_BYTES));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void visitorEnumeratesListRootOnlyWhenListHasAllocatorBackedQuicklistNodes() {
        YierdisDb db = TestDbSupport.open(
                0,
                MaxmemoryPolicy.NOEVICTION,
                1,
                1,
                1
        );
        try {
            db.bindToCurrentThread();
            List<byte[]> values = new ArrayList<>();
            values.add(new byte[4096]);
            values.add(new byte[4096]);
            values.add(new byte[4096]);
            Assert.assertEquals(Long.valueOf(3L), db.writes().lists().rpush(b("list"), values).value());
            Assert.assertEquals(3L,
                    KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.LIST_NODE));

            List<YierdisDbNativeHandleGraph.Role> roles = new ArrayList<>();
            YierdisDbNativeHandleGraph.visitReachable(db.keyLifecycle(), (role, handle, record) -> {
                roles.add(role);
                Assert.assertEquals(KeyLifecycleTestAccess.backend(db).allocatorId(), handle.allocatorId());
                Assert.assertNotNull(record);
            });

            Assert.assertEquals(List.of(
                    YierdisDbNativeHandleGraph.Role.KEY_BYTES,
                    YierdisDbNativeHandleGraph.Role.ENTRY_RECORD,
                    YierdisDbNativeHandleGraph.Role.COLLECTION_ROOT,
                    YierdisDbNativeHandleGraph.Role.COLLECTION_INTERNAL,
                    YierdisDbNativeHandleGraph.Role.COLLECTION_INTERNAL,
                    YierdisDbNativeHandleGraph.Role.COLLECTION_INTERNAL,
                    YierdisDbNativeHandleGraph.Role.COLLECTION_INTERNAL,
                    YierdisDbNativeHandleGraph.Role.COLLECTION_INTERNAL,
                    YierdisDbNativeHandleGraph.Role.COLLECTION_INTERNAL
            ), roles);
            Assert.assertEquals(1L, KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(1L, KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(1L, KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(3L, KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.LIST_NODE));
            Assert.assertEquals(3L, KeyLifecycleTestAccess.backend(db).stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
        } finally {
            db.shutdown();
        }
    }

}
