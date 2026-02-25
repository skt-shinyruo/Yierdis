package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;

import java.nio.charset.StandardCharsets;

public class KeyHandleContractTest {
    @Test
    public void keyHandleEqualityIsContentBasedAcrossBackends() {
        byte[] key = "hello".getBytes(StandardCharsets.US_ASCII);
        KeyHandle heap = KeyHandle.forHeap(key, 123);

        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        long addr = allocator.allocateAddress(key.length);
        try {
            allocator.copyMemory(key, 0, addr, key.length);
            KeyHandle offHeap = KeyHandle.forOffHeap(allocator, addr, key.length, 456);

            Assert.assertEquals(key.length, heap.len());
            Assert.assertEquals(key.length, offHeap.len());
            for (int i = 0; i < key.length; i++) {
                Assert.assertEquals("byte mismatch at " + i, heap.byteAt(i), offHeap.byteAt(i));
            }

            // dictHash 是 keyspace 索引用字段，不参与 equality。
            Assert.assertEquals(123, heap.dictHash());
            Assert.assertEquals(456, offHeap.dictHash());

            Assert.assertEquals(heap, offHeap);
            Assert.assertEquals(offHeap, heap);
            Assert.assertEquals(heap.hashCode(), offHeap.hashCode());
        } finally {
            allocator.freeAddress(addr, key.length);
            allocator.close();
        }
    }

    @Test
    public void keyHandleDistinguishesDifferentKeys() {
        byte[] keyA = "a".getBytes(StandardCharsets.US_ASCII);
        byte[] keyB = "b".getBytes(StandardCharsets.US_ASCII);
        KeyHandle a = KeyHandle.forHeap(keyA, 1);
        KeyHandle b = KeyHandle.forHeap(keyB, 1);
        Assert.assertNotEquals(a, b);
    }
}

