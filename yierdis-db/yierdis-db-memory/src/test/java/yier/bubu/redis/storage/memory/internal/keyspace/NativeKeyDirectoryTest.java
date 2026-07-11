package yier.bubu.redis.storage.memory.internal.keyspace;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NativeKeyDirectoryTest {
    @Test
    public void nativeKeyDirectoryMapsKeysToStableHandlesAndReleasesThem() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-test")) {
            NativeKeyDirectory directory = new NativeKeyDirectory(runtime);
            EntryTable entryTable = new EntryTable(runtime, 64);
            ArrayList<EntryHandle> allocatedEntries = new ArrayList<>();
            try {
                EntryHandle first = entryTable.allocate(entryRecord(1L));
                EntryHandle second = entryTable.allocate(entryRecord(2L));
                allocatedEntries.add(first);
                allocatedEntries.add(second);

                directory.compute(bytes("k1"), (key, old) -> first);
                directory.compute(bytes("k2"), (key, old) -> second);

                Assert.assertEquals(first, directory.get(bytes("k1")));
                Assert.assertEquals(second, directory.get(bytes("k2")));

                for (int i = 0; i < 200; i++) {
                    EntryHandle next = entryTable.allocate(entryRecord(3L + i));
                    allocatedEntries.add(next);
                    directory.compute(bytes("k" + (i + 3)), (key, old) -> next);
                }

                Assert.assertEquals(first, directory.get(bytes("k1")));
                Assert.assertEquals(second, directory.get(bytes("k2")));

                Assert.assertTrue(directory.remove(bytes("k1"), first));
                Assert.assertNull(directory.get(bytes("k1")));
            } finally {
                directory.close();
                allocatedEntries.forEach(entryTable::release);
                entryTable.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void nativeKeyDirectoryRehashesAndFindsAllKeys() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-rehash-test")) {
            NativeKeyDirectory directory = new NativeKeyDirectory(runtime);
            try {
                int count = 300;
                for (int i = 0; i < count; i++) {
                    int slot = i;
                    directory.compute(bytes("key-" + i), (key, old) -> entryHandle(slot + 1L));
                }

                Assert.assertEquals(count, directory.size());
                for (int i = 0; i < count; i++) {
                    byte[] key = bytes("key-" + i);
                    Assert.assertEquals(entryHandle(i + 1L), directory.get(key));
                    Assert.assertArrayEquals(key, copy(directory.getKeyHandle(key)));
                }
            } finally {
                directory.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void nativeKeyDirectoryExposesKeyHandlesForScanAndRandomSelection() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-iteration-test")) {
            NativeKeyDirectory directory = new NativeKeyDirectory(runtime);
            EntryTable entryTable = new EntryTable(runtime, 64);
            ArrayList<EntryHandle> allocatedEntries = new ArrayList<>();
            try {
                EntryHandle first = entryTable.allocate(entryRecord(11L));
                EntryHandle second = entryTable.allocate(entryRecord(12L));
                allocatedEntries.add(first);
                allocatedEntries.add(second);
                directory.compute(bytes("first"), (key, old) -> first);
                directory.compute(bytes("second"), (key, old) -> second);

                KeyHandle firstHandle = directory.getKeyHandle(bytes("first"));
                Assert.assertNotNull(firstHandle);
                Assert.assertArrayEquals(bytes("first"), copy(firstHandle));
                Assert.assertEquals(firstHandle.dictHash(), directory.getKeyHandle(bytes("first")).dictHash());

                KeyHandle random = directory.randomKeyHandle();
                Assert.assertNotNull(random);
                Assert.assertNotNull(KeyHandleAccess.allocatorNativeHandleOrNull(random));
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
                allocatedEntries.forEach(entryTable::release);
                entryTable.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void nativeKeyDirectoryScanResumesAfterCursor() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-scan-resume-test")) {
            NativeKeyDirectory directory = new NativeKeyDirectory(runtime);
            try {
                Map<String, Long> expected = new HashMap<>();
                for (int i = 0; i < 32; i++) {
                    String key = "scan-" + i;
                    EntryHandle handle = entryHandle(i + 1L);
                    expected.put(key, handle.raw());
                    directory.compute(bytes(key), (ignored, old) -> handle);
                }

                Set<String> seen = new HashSet<>();
                ScanCursorV2 cursor = ScanCursorV2.start();
                do {
                    ScanCursorV2 previous = cursor;
                    cursor = directory.scan(cursor, 3, (keyHandle, entryHandle) -> {
                        String key = new String(copy(keyHandle), StandardCharsets.UTF_8);
                        Assert.assertTrue("scan must not revisit keys after resuming from a cursor", seen.add(key));
                        Assert.assertEquals(expected.get(key).longValue(), entryHandle.raw());
                        return true;
                    });
                    if (cursor.value() != 0L) {
                        Assert.assertTrue("scan cursor must advance", cursor.value() > previous.value());
                    }
                } while (cursor.value() != 0L);

                Assert.assertEquals(expected.keySet(), seen);
            } finally {
                directory.close();
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

    @Test
    public void nativeKeyDirectoryRemoveByEntryHandleReleasesKeyAndRemovesMapping() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-remove-entry-handle-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 64)) {
            NativeKeyDirectory directory = new NativeKeyDirectory(allocator);
            try {
                byte[] key = bytes("entry-handle-key");
                EntryHandle handle = entryHandle(81L);
                EntryHandle otherHandle = entryHandle(82L);

                directory.compute(key, (ignored, old) -> handle);
                directory.compute(bytes("other"), (ignored, old) -> otherHandle);
                KeyHandle stored = directory.getKeyHandle(key);
                Assert.assertNotNull(stored);
                Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));

                Assert.assertTrue(directory.remove(handle));
                Assert.assertNull(directory.get(key));
                Assert.assertNull(directory.getKeyHandle(key));
                Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));
                try {
                    stored.length();
                    Assert.fail("entry handle removal must release the native key handle");
                } catch (RuntimeException expected) {
                    Assert.assertTrue(expected.getMessage().contains("stale native handle"));
                }

                Assert.assertFalse(directory.remove(handle));
                Assert.assertEquals(otherHandle, directory.get(bytes("other")));
            } finally {
                directory.close();
            }

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));
        }
    }

    @Test
    public void nativeKeyDirectoryStoresKeysAsAllocatorKeyBytesAndRejectsFreedHandles() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-key-bytes-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 64)) {
            NativeKeyDirectory directory = new NativeKeyDirectory(allocator);
            try {
                byte[] key = bytes("native-key");
                EntryHandle handle = entryHandle(1L);

                directory.compute(key, (ignored, old) -> handle);

                KeyHandle stored = directory.getKeyHandle(key);
                Assert.assertNotNull(stored);
                Assert.assertArrayEquals(key, copy(stored));
                Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertEquals(key.length, allocator.stats().logicalUsedBytes());
                Assert.assertEquals("KEY_BYTES are reported by the allocator, not the directory side channel",
                        0L, directory.nativeBytes());

                Assert.assertTrue(directory.remove(key, handle));
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));
                try {
                    stored.length();
                    Assert.fail("freed key handle should fail through allocator stale-handle checks");
                } catch (RuntimeException expected) {
                    Assert.assertTrue(expected.getMessage().contains("stale native handle"));
                }
            } finally {
                directory.close();
            }

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.KEY_BYTES));
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
        byte[] out = new byte[keyHandle.length()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.getByte(i);
        }
        return out;
    }

    private static boolean equalsBytes(KeyHandle keyHandle, byte[] bytes) {
        if (keyHandle.length() != bytes.length) {
            return false;
        }
        for (int i = 0; i < bytes.length; i++) {
            if (keyHandle.getByte(i) != bytes[i]) {
                return false;
            }
        }
        return true;
    }
}
