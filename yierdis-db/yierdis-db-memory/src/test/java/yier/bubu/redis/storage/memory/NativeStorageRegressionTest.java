package yier.bubu.redis.storage.memory;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class NativeStorageRegressionTest {
    @Test
    public void allNativeRootsReleaseToZeroAfterDelete() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            Assert.assertTrue(db.writes().strings().setString(b("s"), b("v"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("l"), List.of(b("a"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("h"), List.of(b("f"), b("v"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("m"))).value());
            Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("z"), List.of(b("1"), b("m"))).value());
            Assert.assertEquals(Integer.valueOf(1), db.writes().hll().pfadd(b("hll"), List.of(b("x"))).value());

            Assert.assertEquals(6, db.size());
            Assert.assertTrue(db.memory().memoryStats().usedBytesForMaxmemory() > 0);

            Assert.assertEquals(Long.valueOf(6L), db.writes().keyspace().del(List.of(
                    b("s"),
                    b("l"),
                    b("h"),
                    b("set"),
                    b("z"),
                    b("hll")
            )).value());

            YierdisMemoryStats stats = db.memory().memoryStats();
            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, db.usedBytesForMaxmemory());
            Assert.assertEquals(0L, stats.usedBytesForMaxmemory());
            Assert.assertEquals(0L, stats.heapDataBytesEstimate());
            Assert.assertEquals(0L, stats.offHeapUsedBytes());
            Assert.assertEquals(0L, stats.totalEstimatedBytes());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void deleteUsesEntryMetadataInsteadOfCompatibilityObjectEstimate() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("set"), List.of(b("m"))).value());
            long before = db.usedBytesForMaxmemory();
            Assert.assertTrue(before > 0);

            Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(b("set"))).value());

            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, db.usedBytesForMaxmemory());
            Assert.assertEquals(0L, db.memory().memoryStats().usedBytesForMaxmemory());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void keyLifecycleReadsKeysFromNativeDirectoryWithoutCompatibilityStoreEntry() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
            byte[] key = b("native-only");
            EntryRecord record = new EntryRecord(
                    1L,
                    ValueHandle.NULL,
                    31,
                    ValueType.STRING,
                    ValueEncoding.STRING_RAW,
                    0,
                    -1L,
                    64L,
                    0L
            );
            EntryHandle handle = lifecycle.entryTable().allocate(record);
            lifecycle.keyDirectory().compute(key, (ignored, oldHandle) -> handle);

            Assert.assertEquals(1, lifecycle.keyCount());
            Assert.assertEquals(1, db.size());
            Assert.assertNotNull(lifecycle.keyHandle(key));
            Assert.assertNotNull(lifecycle.liveEntryRecord(key));
            Assert.assertArrayEquals(key, copy(lifecycle.randomKeyHandle()));

            List<String> scanned = new ArrayList<>();
            ScanCursorV2 cursor = lifecycle.scan(ScanCursorV2.start(), 16, (keyHandle, scannedRecord) -> {
                scanned.add(new String(copy(keyHandle), StandardCharsets.UTF_8));
                Assert.assertNotNull(scannedRecord);
                return true;
            });

            Assert.assertEquals(0L, cursor.value());
            Assert.assertEquals(List.of("native-only"), scanned);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] stringKey = b("native-string");
            byte[] counterKey = b("native-counter");

            Assert.assertTrue(db.writes().strings().setString(stringKey, b("hello"), SetMode.NORMAL, null).value());
            assertNativeStringOnly(db, stringKey, b("hello"));
            Assert.assertTrue(db.reads().keyspace().existsKey(view(stringKey)));
            Assert.assertEquals(ValueType.STRING, db.reads().keyspace().typeOf(view(stringKey)));
            Assert.assertEquals(List.of("native-string"), strings(db.reads().keyspace().keys(b("native-*"), 16, 0)));

            List<byte[]> scanned = new ArrayList<>();
            ScanCursorV2 cursor = db.reads().keyspace().scan(ScanCursorV2.start(), b("native-*"), 16, scanned);
            Assert.assertEquals(0L, cursor.value());
            Assert.assertEquals(List.of("native-string"), strings(scanned));

            Assert.assertEquals(Long.valueOf(11L), db.writes().strings().append(stringKey, sliceOf(b(" world"))).value());
            assertNativeStringOnly(db, stringKey, b("hello world"));

            Assert.assertEquals(Integer.valueOf(0), db.writes().strings().setBit(stringKey, 0, 1).value());
            Assert.assertEquals(1, db.reads().strings().getBit(view(stringKey), 0));
            assertNativeStringOnly(db, stringKey, db.reads().strings().getStringBytes(stringKey));

            Assert.assertTrue(db.writes().strings().setString(counterKey, b("41"), SetMode.NORMAL, null).value());
            Assert.assertEquals(Long.valueOf(42L), db.writes().strings().incrBy(counterKey, 1L).value());
            assertNativeStringOnly(db, counterKey, b("42"));

            EntryRecord counterRecord = db.keyLifecycle().liveEntryRecord(counterKey);
            Assert.assertEquals(ValueEncoding.STRING_INT, counterRecord.encoding());

            Assert.assertEquals(Long.valueOf(2L), db.writes().keyspace().del(List.of(stringKey, counterKey)).value());
            Assert.assertEquals(0, db.size());
            Assert.assertEquals(0L, db.usedBytesForMaxmemory());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void nativeDbChurnKeepsReporterAndRuntimeAccountingConsistent() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-db-churn")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    2_000_000L,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            db.bindToCurrentThread();
            try {
                List<byte[]> keys = new ArrayList<>();
                for (int i = 0; i < 64; i++) {
                    byte[] key = b("churn-" + i);
                    keys.add(key);
                    Assert.assertTrue(db.writes().strings().setString(
                            key,
                            b("value-" + i),
                            SetMode.NORMAL,
                            null
                    ).value());
                    if ((i & 1) == 0) {
                        Assert.assertEquals(Long.valueOf(("value-" + i + "-tail").length()),
                                db.writes().strings().append(key, sliceOf(b("-tail"))).value());
                    }
                    if (i % 5 == 0) {
                        Assert.assertTrue(db.writes().ttl().pexpire(view(key), 60_000L).value());
                    }
                }

                YierdisMemoryStats populated = db.memory().memoryStats();
                Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES) > 0L);
                Assert.assertEquals(db.size(),
                        db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertTrue(db.keyLifecycle().nativeAllocator().stats().logicalUsedBytes() > 0L);
                Assert.assertEquals(db.size(), populated.keyCount());
                Assert.assertEquals(db.usedBytesForMaxmemory(), populated.usedBytesForMaxmemory());
                Assert.assertTrue(populated.offHeapUsedBytes() > 0L);

                List<byte[]> evens = new ArrayList<>();
                for (int i = 0; i < keys.size(); i += 2) {
                    evens.add(keys.get(i));
                }
                Assert.assertEquals(Long.valueOf(evens.size()), db.writes().keyspace().del(evens).value());

                YierdisMemoryStats afterDelete = db.memory().memoryStats();
                Assert.assertEquals(db.size(), afterDelete.keyCount());
                Assert.assertEquals(db.usedBytesForMaxmemory(), afterDelete.usedBytesForMaxmemory());
                Assert.assertTrue(afterDelete.usedBytesForMaxmemory() <= populated.usedBytesForMaxmemory());

                Assert.assertEquals(Long.valueOf(keys.size() - evens.size()), db.writes().keyspace().del(keys).value());
                Assert.assertEquals(0, db.size());
                Assert.assertEquals(0L, db.memory().memoryStats().usedBytesForMaxmemory());
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.STRING_BYTES));
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertEquals(0L, db.keyLifecycle().nativeAllocator().stats().logicalUsedBytes());
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static void assertNativeStringOnly(YierdisDb db, byte[] key, byte[] expectedBytes) {
        YierdisDbKeyLifecycle lifecycle = db.keyLifecycle();
        KeyHandle keyHandle = lifecycle.keyHandle(key);
        Assert.assertNotNull(keyHandle);
        EntryRecord record = lifecycle.liveEntryRecord(key);
        Assert.assertNotNull(record);
        Assert.assertEquals(ValueType.STRING, record.type());
        Assert.assertArrayEquals(expectedBytes, lifecycle.stringRoot().copy(record.valueHandle()));
        Assert.assertArrayEquals(expectedBytes, db.reads().strings().getStringBytes(key));
        Assert.assertEquals(expectedBytes.length, db.reads().strings().strlen(view(key)));
    }

    private static byte[] copy(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.len()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
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

    private static BytesSlice sliceOf(byte[] data) {
        return new BytesSlice() {
            @Override
            public void writeTo(BytesSink out) {
                out.writeBytes(data, 0, data.length);
            }

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

    private static List<String> strings(List<byte[]> values) {
        List<String> out = new ArrayList<>(values.size());
        for (byte[] value : values) {
            out.add(new String(value, StandardCharsets.UTF_8));
        }
        return out;
    }
}
