package yier.bubu.redis.storage.memory.internal.keyspace;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class NativeKeyDirectoryTest {
    @Test
    public void nativeKeyDirectoryMapsKeysToStableHandlesAndReleasesThem() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-test")) {
            NativeKeyDirectory directory = new NativeKeyDirectory(runtime);
            EntryTable entryTable = new EntryTable(runtime, 64);
            try {
                EntryHandle first = entryTable.allocate(entryRecord(1L));
                EntryHandle second = entryTable.allocate(entryRecord(2L));

                directory.compute(bytes("k1"), (key, old) -> first);
                directory.compute(bytes("k2"), (key, old) -> second);

                Assert.assertEquals(first, directory.get(bytes("k1")));
                Assert.assertEquals(second, directory.get(bytes("k2")));

                for (int i = 0; i < 200; i++) {
                    EntryHandle next = entryTable.allocate(entryRecord(3L + i));
                    directory.compute(bytes("k" + (i + 3)), (key, old) -> next);
                }

                Assert.assertEquals(first, directory.get(bytes("k1")));
                Assert.assertEquals(second, directory.get(bytes("k2")));

                Assert.assertTrue(directory.remove(bytes("k1"), first));
                Assert.assertNull(directory.get(bytes("k1")));
            } finally {
                directory.close();
                entryTable.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void nativeKeyDirectoryExposesKeyHandlesForScanAndRandomSelection() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-iteration-test")) {
            NativeKeyDirectory directory = new NativeKeyDirectory(runtime);
            EntryTable entryTable = new EntryTable(runtime, 64);
            try {
                EntryHandle first = entryTable.allocate(entryRecord(11L));
                EntryHandle second = entryTable.allocate(entryRecord(12L));
                directory.compute(bytes("first"), (key, old) -> first);
                directory.compute(bytes("second"), (key, old) -> second);

                KeyHandle firstHandle = directory.getKeyHandle(bytes("first"));
                Assert.assertNotNull(firstHandle);
                Assert.assertArrayEquals(bytes("first"), copy(firstHandle));
                Assert.assertEquals(firstHandle.dictHash(), directory.getKeyHandle(bytes("first")).dictHash());

                KeyHandle random = directory.randomKeyHandle();
                Assert.assertNotNull(random);
                Assert.assertTrue(
                        "random key must come from the directory",
                        equalsBytes(random, bytes("first")) || equalsBytes(random, bytes("second"))
                );

                Map<String, Long> seen = new HashMap<>();
                directory.forEachEntry((keyHandle, entryHandle) ->
                        seen.put(new String(copy(keyHandle), StandardCharsets.UTF_8), entryHandle.raw()));

                Assert.assertEquals(Map.of("first", first.raw(), "second", second.raw()), seen);

                Map<String, Long> scanned = new HashMap<>();
                ScanCursorV2 cursor = ScanCursorV2.start();
                do {
                    cursor = directory.scan(cursor, 1, (keyHandle, entryHandle) -> {
                        scanned.put(new String(copy(keyHandle), StandardCharsets.UTF_8), entryHandle.raw());
                        return true;
                    });
                } while (cursor.value() != 0L);

                Assert.assertEquals(Map.of("first", first.raw(), "second", second.raw()), scanned);
                Assert.assertNull(directory.getKeyHandle(bytes("missing")));
            } finally {
                directory.close();
                entryTable.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void nativeKeyDirectoryScanCanStopEarlyAndRandomKeyIsNullWhenEmpty() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-scan-stop-test")) {
            NativeKeyDirectory directory = new NativeKeyDirectory(runtime);
            try {
                Assert.assertNull(directory.randomKeyHandle());
                Assert.assertEquals(0L, directory.scan(ScanCursorV2.start(), 8, (key, handle) -> true).value());

                directory.compute(bytes("one"), (key, old) -> entryHandle(1L));
                directory.compute(bytes("two"), (key, old) -> entryHandle(2L));

                int[] visited = {0};
                ScanCursorV2 cursor = directory.scan(ScanCursorV2.start(), 8, (key, handle) -> {
                    visited[0]++;
                    return false;
                });

                Assert.assertEquals(1, visited[0]);
                Assert.assertTrue("scan cursor must advance after an early stop", cursor.value() > 0L);
            } finally {
                directory.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static EntryRecord entryRecord(long keyHandle) {
        return new EntryRecord(
                keyHandle,
                ValueHandle.NULL,
                1,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                0,
                -1L,
                0L,
                0L
        );
    }

    private static EntryHandle entryHandle(long slotId) {
        NativeObjectKind kind = NativeObjectKind.ENTRY_RECORD;
        return EntryHandle.fromNativeHandle(NativeHandle.of(kind.domain(), kind, slotId, 1, 0));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] copy(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.len()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
    }

    private static boolean equalsBytes(KeyHandle keyHandle, byte[] bytes) {
        if (keyHandle.len() != bytes.length) {
            return false;
        }
        for (int i = 0; i < bytes.length; i++) {
            if (keyHandle.byteAt(i) != bytes[i]) {
                return false;
            }
        }
        return true;
    }
}
