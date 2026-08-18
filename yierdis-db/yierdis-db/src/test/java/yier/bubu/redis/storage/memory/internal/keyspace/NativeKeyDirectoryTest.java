package yier.bubu.redis.storage.memory.internal.keyspace;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.DbThreadGuard;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.memory.internal.keyspace.TestNativeKeyDirectories.insert;

public class NativeKeyDirectoryTest {
    private static final HashSeed FIXED_SEED = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);

    @Test
    public void equalLocalRawFromAnotherBackendCannotAliasAnEntry() {
        HeapStableMemoryBackend left = new HeapStableMemoryBackend("directory-left", 8, new DbThreadGuard());
        HeapStableMemoryBackend right = new HeapStableMemoryBackend("directory-right", 8, new DbThreadGuard());
        left.bindToCurrentThread();
        right.bindToCurrentThread();
        NativeHandle leftNative = left.allocate(NativeObjectKind.ENTRY_RECORD, 1);
        NativeHandle rightNative = right.allocate(NativeObjectKind.ENTRY_RECORD, 1);
        try (NativeKeyDirectory directory = new NativeKeyDirectory(left, FIXED_SEED, new HashTableMaintenanceRegistry())) {
            EntryHandle local = new EntryHandle(leftNative);
            EntryHandle foreign = new EntryHandle(rightNative);
            insert(directory, b("collision"), local);
            Assert.assertFalse(directory.remove(b("collision"), foreign));
            Assert.assertEquals(local, directory.get(b("collision")));
        } finally {
            left.free(leftNative);
            right.free(rightNative);
            left.close();
            right.close();
        }
    }

    @Test
    public void directoryOwnsKeyBytesAndRemovesMappingWithoutOwningEntry() {
        try (TestBackend runtime = TestBackend.open("directory-lifecycle")) {
            StableMemoryBackend backend = runtime.backend();
            NativeKeyDirectory directory = new NativeKeyDirectory(backend, FIXED_SEED, new HashTableMaintenanceRegistry());
            NativeHandle entryNative = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
            EntryHandle entry = new EntryHandle(entryNative);
            try {
                Assert.assertEquals(entry, insert(directory, b("key"), entry));
                Assert.assertEquals(1, directory.size());
                Assert.assertNotNull(directory.getKeyHandle(b("key")));
                Assert.assertEquals(entry, directory.remove(b("key")));
                Assert.assertEquals(0, directory.size());
                Assert.assertNull(directory.randomKeyHandle());
            } finally {
                directory.close();
                backend.free(entryNative);
            }
        }
    }

    @Test
    public void rehashAndScanExposeEveryStableEntry() {
        try (TestBackend runtime = TestBackend.open("directory-rehash")) {
            StableMemoryBackend backend = runtime.backend();
            NativeKeyDirectory directory = new NativeKeyDirectory(backend, FIXED_SEED, new HashTableMaintenanceRegistry());
            List<NativeHandle> entries = new ArrayList<>();
            try {
                for (int i = 0; i < 40; i++) {
                    NativeHandle nativeEntry = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
                    entries.add(nativeEntry);
                    EntryHandle entry = new EntryHandle(nativeEntry);
                    insert(directory, b("key-" + i), entry);
                }
                Assert.assertEquals(40, directory.size());
                List<String> seen = new ArrayList<>();
                ScanCursorV2 cursor = ScanCursorV2.start();
                do {
                    var result = directory.scanWithWork(cursor, 32L, (key, entry) -> {
                        seen.add(new String(copy(key), java.nio.charset.StandardCharsets.US_ASCII));
                        return true;
                    });
                    cursor = result.nextCursor();
                    if (cursor.value() == 0L) {
                        break;
                    }
                } while (seen.size() < 40);
                Assert.assertEquals(40, seen.size());
            } finally {
                directory.close();
                for (NativeHandle entry : entries) {
                    backend.free(entry);
                }
            }
        }
    }

    @Test
    public void detachDuringRehashReclaimsEachOldKeyOnceAndKeepsTheNewGeneration() {
        try (TestBackend runtime = TestBackend.open("directory-detach-rehash")) {
            StableMemoryBackend backend = runtime.backend();
            NativeKeyDirectory directory = new NativeKeyDirectory(backend, FIXED_SEED, new HashTableMaintenanceRegistry());
            List<NativeHandle> oldEntries = new ArrayList<>();
            NativeHandle newEntry = null;
            try {
                for (int index = 0; index < 256 && !directory.metrics().rehashing(); index++) {
                    NativeHandle nativeEntry = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
                    oldEntries.add(nativeEntry);
                    insert(directory, b("key-" + index), new EntryHandle(nativeEntry));
                }
                Assert.assertTrue("test setup must leave the directory rehashing", directory.metrics().rehashing());
                while (directory.advanceRehash(HashTableWorkBudget.of(1L, Long.MAX_VALUE)).migratedSlots() == 0L) {
                    Assert.assertTrue(directory.metrics().rehashing());
                }

                int oldKeyCount = directory.size();
                directory.detachEntries();
                Assert.assertEquals(0, directory.size());
                Assert.assertEquals(oldKeyCount, directory.detachedEntryCount());

                newEntry = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
                EntryHandle newHandle = new EntryHandle(newEntry);
                insert(directory, b("key-0"), newHandle);

                Set<NativeHandle> reclaimedEntries = new HashSet<>();
                while (directory.detachedEntryCount() > 0) {
                    Assert.assertTrue(directory.reclaimDetachedEntry((ignored, entry) ->
                            Assert.assertTrue(
                                    "a migrated scan shadow must not be reclaimed twice",
                                    reclaimedEntries.add(entry.nativeHandle())
                            )));
                }

                Assert.assertEquals(oldKeyCount, reclaimedEntries.size());
                Assert.assertEquals(newHandle, directory.get(b("key-0")));
            } finally {
                directory.close();
                for (NativeHandle entry : oldEntries) {
                    backend.free(entry);
                }
                if (newEntry != null) {
                    backend.free(newEntry);
                }
            }
        }
    }

    private static byte[] copy(yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle key) {
        byte[] bytes = new byte[key.length()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = key.getByte(i);
        }
        return bytes;
    }
}
