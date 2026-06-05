package yier.bubu.redis.storage.memory.internal.key;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

import java.nio.charset.StandardCharsets;

public class KeyHandleContractTest {
    @Test
    public void nativeKeyHandleIsReadOnlyBytesViewWithStableDictHash() {
        byte[] key = "hello".getBytes(StandardCharsets.US_ASCII);
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-key-handle-contract");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            EntryHandle entry = EntryHandle.fromNativeHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                directory.compute(key, (ignored, old) -> entry);

                KeyHandle handle = directory.getKeyHandle(key);
                Assert.assertNotNull(handle);
                Assert.assertEquals(key.length, handle.len());
                for (int i = 0; i < key.length; i++) {
                    Assert.assertEquals(key[i], handle.byteAt(i));
                }
                Assert.assertEquals(handle.dictHash(), directory.getKeyHandle(key).dictHash());
                Assert.assertTrue(KeyHandleAccess.isAllocator(handle));
                Assert.assertNotNull(KeyHandleAccess.allocatorNativeHandle(handle));
            } finally {
                allocator.free(entry.nativeHandle());
            }
        }
    }

    @Test
    public void keyHandleDistinguishesDifferentKeys() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-key-handle-distinct");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            EntryHandle aEntry = EntryHandle.fromNativeHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            EntryHandle bEntry = EntryHandle.fromNativeHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                byte[] keyA = "a".getBytes(StandardCharsets.US_ASCII);
                byte[] keyB = "b".getBytes(StandardCharsets.US_ASCII);
                directory.compute(keyA, (ignored, old) -> aEntry);
                directory.compute(keyB, (ignored, old) -> bEntry);

                Assert.assertNotEquals(directory.getKeyHandle(keyA), directory.getKeyHandle(keyB));
            } finally {
                allocator.free(aEntry.nativeHandle());
                allocator.free(bEntry.nativeHandle());
            }
        }
    }
}
