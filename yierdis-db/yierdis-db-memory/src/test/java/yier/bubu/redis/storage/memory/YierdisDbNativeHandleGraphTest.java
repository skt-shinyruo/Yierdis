package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
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
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(
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
            EnumMap<NativeObjectKind, Integer> kindCounts = new EnumMap<>(NativeObjectKind.class);

            YierdisDbNativeHandleGraph.visitReachable(db.keyLifecycle(), (role, handle, record) -> {
                roleCounts.merge(role, 1, Integer::sum);
                kindCounts.merge(nativeKind(handle), 1, Integer::sum);
                Assert.assertNotNull(record);
            });

            Assert.assertEquals(Integer.valueOf(5), roleCounts.get(YierdisDbNativeHandleGraph.Role.KEY_BYTES));
            Assert.assertEquals(Integer.valueOf(5), roleCounts.get(YierdisDbNativeHandleGraph.Role.ENTRY_RECORD));
            Assert.assertEquals(Integer.valueOf(1), roleCounts.get(YierdisDbNativeHandleGraph.Role.STRING_VALUE));
            Assert.assertEquals(Integer.valueOf(4), roleCounts.get(YierdisDbNativeHandleGraph.Role.COLLECTION_ROOT));
            Assert.assertEquals(Integer.valueOf(5), roleCounts.get(YierdisDbNativeHandleGraph.Role.COLLECTION_INTERNAL));

            Assert.assertEquals(Integer.valueOf(5), kindCounts.get(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(Integer.valueOf(5), kindCounts.get(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.STRING_BYTES));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.HASH_ROOT));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.SET_ROOT));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.ZSET_ROOT));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.LISTPACK_BYTES));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.HASH_FIELD_BYTES));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.HASH_VALUE_BYTES));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.SET_MEMBER_BYTES));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.ZSET_MEMBER_BYTES));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void visitorEnumeratesListRootOnlyWhenListHasAllocatorBackedQuicklistNodes() {
        YierdisDb db = YierdisDb.createWithOwnedFfmRuntime(
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
                    db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.LIST_NODE));

            List<YierdisDbNativeHandleGraph.Role> roles = new ArrayList<>();
            EnumMap<NativeObjectKind, Integer> kindCounts = new EnumMap<>(NativeObjectKind.class);

            YierdisDbNativeHandleGraph.visitReachable(db.keyLifecycle(), (role, handle, record) -> {
                roles.add(role);
                kindCounts.merge(nativeKind(handle), 1, Integer::sum);
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
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(Integer.valueOf(3), kindCounts.get(NativeObjectKind.LIST_NODE));
            Assert.assertEquals(Integer.valueOf(3), kindCounts.get(NativeObjectKind.LISTPACK_BYTES));
        } finally {
            db.shutdown();
        }
    }

    private static NativeObjectKind nativeKind(NativeHandle handle) {
        for (NativeObjectKind kind : NativeObjectKind.values()) {
            if (kind.domain() == handle.domain() && kind.code() == handle.kindCode()) {
                return kind;
            }
        }
        throw new AssertionError("unknown native object kind: " + handle.raw());
    }
}
