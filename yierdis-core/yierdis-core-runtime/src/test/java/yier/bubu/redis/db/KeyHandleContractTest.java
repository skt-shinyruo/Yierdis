package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.memory.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.db.memory.ffm.YierdisFfmBytesRef;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;

import java.nio.charset.StandardCharsets;

public class KeyHandleContractTest {
    @Test
    public void keyHandleEqualityIsContentBasedAcrossHeapAndFfm() {
        byte[] key = "hello".getBytes(StandardCharsets.US_ASCII);
        KeyHandle heap = KeyHandle.forHeap(key, 123);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test")) {
            YierdisFfmBytesRef ref = YierdisFfmBlobStore.fromBytes(runtime, key);
            try {
                KeyHandle ffm = KeyHandle.forFfm(ref, 456);
                Assert.assertEquals(key.length, heap.len());
                Assert.assertEquals(key.length, ffm.len());
                for (int i = 0; i < key.length; i++) {
                    Assert.assertEquals("byte mismatch at " + i, heap.byteAt(i), ffm.byteAt(i));
                }

                // dictHash 是 keyspace 索引用字段，不参与 equality。
                Assert.assertEquals(123, heap.dictHash());
                Assert.assertEquals(456, ffm.dictHash());

                Assert.assertEquals(heap, ffm);
                Assert.assertEquals(ffm, heap);
                Assert.assertEquals(heap.hashCode(), ffm.hashCode());
            } finally {
                ref.region().close();
            }
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
