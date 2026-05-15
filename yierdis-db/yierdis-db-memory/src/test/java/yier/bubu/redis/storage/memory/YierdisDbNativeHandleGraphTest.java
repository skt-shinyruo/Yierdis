package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

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

            Assert.assertEquals(Integer.valueOf(5), kindCounts.get(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(Integer.valueOf(5), kindCounts.get(NativeObjectKind.ENTRY_RECORD));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.STRING_BYTES));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.LIST_NODE));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.HASH_NODE));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.SET_NODE));
            Assert.assertEquals(Integer.valueOf(1), kindCounts.get(NativeObjectKind.ZSET_NODE));
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
