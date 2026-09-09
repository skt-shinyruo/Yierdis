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
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.memory.internal.keyspace.TestNativeKeyDirectories.insert;

public class NativeKeyDirectoryTest {
    private static final HashSeed FIXED_SEED = new HashSeed(0x0123456789abcdefL, 0xfedcba9876543210L);

    @Test
    public void growthEstimateIncludesTheStagedReplacementAndSharedTopology() {
        try (TestBackend runtime = TestBackend.open("directory-growth-estimate")) {
            StableMemoryBackend backend = runtime.backend();
            NativeKeyDirectory directory = new NativeKeyDirectory(backend, FIXED_SEED, null);
            List<NativeHandle> entries = new ArrayList<>();
            try {
                for (int i = 0; i < 12; i++) {
                    NativeHandle nativeEntry = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
                    entries.add(nativeEntry);
                    insert(directory, b("growth-" + i), new EntryHandle(nativeEntry));
                }

                long payloadBytes = 48L + 2L * (16L + 32L * Long.BYTES);
                long topologyBytes = 64L + 48L + 16L + 32L * Integer.BYTES + 16L + 32L;
                Assert.assertEquals(
                        32L + payloadBytes + topologyBytes,
                        directory.estimatedInsertHeapGrowthBytes()
                );
            } finally {
                directory.close();
                for (NativeHandle entry : entries) {
                    backend.free(entry);
                }
            }
        }
    }

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

    @Test
    public void removeEntryByStoredIdentityLocatesEntriesAcrossMidRehashTables() {
        try (TestBackend runtime = TestBackend.open("directory-remove-entry-mid-rehash")) {
            StableMemoryBackend backend = runtime.backend();
            NativeKeyDirectory directory = new NativeKeyDirectory(backend, FIXED_SEED, new HashTableMaintenanceRegistry());
            List<NativeHandle> entries = new ArrayList<>();
            try {
                int inserted = 0;
                while (inserted < 256 && !directory.metrics().rehashing()) {
                    NativeHandle nativeEntry = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
                    entries.add(nativeEntry);
                    insert(directory, b("key-" + inserted), new EntryHandle(nativeEntry));
                    inserted++;
                }
                Assert.assertTrue("test setup must leave the directory rehashing", directory.metrics().rehashing());
                // 迁移一部分槽位，让目录同时存在 old-resident 与已迁移（active + old shadow）的 entry。
                directory.advanceRehash(HashTableWorkBudget.of(8L, Long.MAX_VALUE));
                Assert.assertTrue(directory.metrics().rehashing());

                for (int i = 0; i < inserted; i++) {
                    byte[] key = b("key-" + i);
                    AllocatorKeyHandle keyHandle = directory.getKeyHandle(key);
                    Assert.assertNotNull(keyHandle);
                    EntryHandle entry = new EntryHandle(entries.get(i));
                    Assert.assertTrue(directory.removeEntry(keyHandle.nativeHandle(), keyHandle.dictHash(), entry));
                    Assert.assertNull(directory.get(key));
                    Assert.assertFalse(directory.removeEntry(keyHandle.nativeHandle(), keyHandle.dictHash(), entry));
                }
                Assert.assertEquals(0, directory.size());
                List<String> survivors = new ArrayList<>();
                directory.scanWithWork(ScanCursorV2.start(), Long.MAX_VALUE, (key, entry) -> {
                    survivors.add("x");
                    return true;
                });
                Assert.assertTrue(survivors.isEmpty());
            } finally {
                directory.close();
                for (NativeHandle entry : entries) {
                    backend.free(entry);
                }
            }
        }
    }

    @Test
    public void removeEntryByStoredIdentityRejectsAMismatchedEntry() {
        try (TestBackend runtime = TestBackend.open("directory-remove-entry-mismatch")) {
            StableMemoryBackend backend = runtime.backend();
            NativeKeyDirectory directory = new NativeKeyDirectory(backend, FIXED_SEED, new HashTableMaintenanceRegistry());
            NativeHandle entryNative = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
            NativeHandle otherNative = backend.allocate(NativeObjectKind.ENTRY_RECORD, 1);
            EntryHandle entry = new EntryHandle(entryNative);
            try {
                insert(directory, b("key"), entry);
                AllocatorKeyHandle keyHandle = directory.getKeyHandle(b("key"));
                Assert.assertFalse(directory.removeEntry(
                        keyHandle.nativeHandle(),
                        keyHandle.dictHash(),
                        new EntryHandle(otherNative)
                ));
                Assert.assertEquals(entry, directory.get(b("key")));
                Assert.assertEquals(1, directory.size());
            } finally {
                directory.close();
                backend.free(entryNative);
                backend.free(otherNative);
            }
        }
    }

    @Test
    public void removeEntryByStoredIdentityCannotAliasAKeyHandleFromAnotherBackend() {
        HeapStableMemoryBackend left = new HeapStableMemoryBackend("directory-remove-left", 8, new DbThreadGuard());
        HeapStableMemoryBackend right = new HeapStableMemoryBackend("directory-remove-right", 8, new DbThreadGuard());
        left.bindToCurrentThread();
        right.bindToCurrentThread();
        NativeHandle entryNative = left.allocate(NativeObjectKind.ENTRY_RECORD, 1);
        NativeHandle foreignKey = right.allocate(NativeObjectKind.KEY_BYTES, 1);
        EntryHandle entry = new EntryHandle(entryNative);
        try (NativeKeyDirectory directory = new NativeKeyDirectory(left, FIXED_SEED, new HashTableMaintenanceRegistry())) {
            insert(directory, b("collision"), entry);
            AllocatorKeyHandle keyHandle = directory.getKeyHandle(b("collision"));
            Assert.assertFalse(directory.removeEntry(foreignKey, keyHandle.dictHash(), entry));
            Assert.assertEquals(entry, directory.get(b("collision")));
            Assert.assertEquals(1, directory.size());
        } finally {
            left.free(entryNative);
            right.free(foreignKey);
            left.close();
            right.close();
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
