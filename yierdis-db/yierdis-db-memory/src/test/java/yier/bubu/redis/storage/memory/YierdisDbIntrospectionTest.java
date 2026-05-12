package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class YierdisDbIntrospectionTest {
    @Test
    public void objectEncodingReadsNativeEntryEncoding() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("introspection-encoding")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            db.bindToCurrentThread();
            try {
                byte[] key = bytes("encoding-key");
                db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);

                Assert.assertEquals("raw", db.memory().objectEncoding(view(key)));
                Assert.assertNull(db.memory().objectEncoding(view(bytes("missing"))));
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void snapshotCopiesNativeStringValueAndExpireMetadata() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("introspection-snapshot")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            db.bindToCurrentThread();
            try {
                byte[] key = bytes("snapshot-key");
                byte[] value = bytes("snapshot-value");
                db.writes().strings().setString(key, value, SetMode.NORMAL, null);
                db.writes().ttl().pexpire(view(key), 60_000L);

                List<YierdisSnapshotEntry> entries = new ArrayList<>();
                ScanCursorV2 cursor = db.introspection().snapshot(ScanCursorV2.start(), 10, entries);

                Assert.assertNotNull(cursor);
                Assert.assertEquals(1, entries.size());
                YierdisSnapshotEntry entry = entries.get(0);
                Assert.assertArrayEquals(key, entry.keyBytes());
                Assert.assertEquals(ValueType.STRING, entry.type());
                Assert.assertArrayEquals(value, entry.stringValueBytes());
                Assert.assertNotNull(entry.expireAtMillis());
                Assert.assertTrue(entry.expireAtMillis() > System.currentTimeMillis());
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static BytesView view(byte[] data) {
        return new BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }
}
