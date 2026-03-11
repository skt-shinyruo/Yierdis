package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.ops.SetMode;

import static yier.bubu.redis.testutil.TestBytes.b;

public class OffHeapBytesViewTtlRegressionTest {
    @Test
    public void pexpireBytesViewDoesNotTriggerExpireIndexContentScanInOffHeapKeysMode() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, true, 0, "noeviction", 5, 5, 5);
        byte[] targetKey = b("k00000");
        int targetKeyLen = targetKey.length;
        long[] dummyKeyAddrs = null;
        try {
            db.bindToCurrentThread();
            byte[] emptyValue = new byte[0];

            db.setString(targetKey, emptyValue, SetMode.NORMAL, null);

            // Populate the off-heap expire index without growing the keyspace:
            // `YierdisUnsafeOffHeapExpireIndex.get(BytesView)` uses a full-table content scan, so a large TTL set will
            // amplify any accidental call to `expires.get(keyView)` into an O(n) path.
            int dummyTtlEntries = 8_000;
            long nowMillis = System.currentTimeMillis();
            byte[] dummyKeyBytes = new byte[targetKeyLen];
            dummyKeyBytes[0] = (byte) 'x'; // ensure mismatch against "k00000" quickly (1 byteAt per slot)
            dummyKeyAddrs = new long[dummyTtlEntries];
            for (int i = 0; i < dummyTtlEntries; i++) {
                dummyKeyBytes[targetKeyLen - 1] = (byte) i;
                long addr = allocator.allocateAddress(targetKeyLen);
                dummyKeyAddrs[i] = addr;
                allocator.copyMemory(dummyKeyBytes, 0, addr, targetKeyLen);
                KeyHandle handle = KeyHandle.forOffHeap(allocator, addr, targetKeyLen, i);
                db.expires.setExpireAtMillis(handle, nowMillis + 60_000);
            }

            final int[] reads = new int[]{0};
            BytesView view = new BytesView() {
                @Override
                public int length() {
                    return targetKey.length;
                }

                @Override
                public byte getByte(int index) {
                    reads[0]++;
                    if (reads[0] > 2_000) {
                        throw new IllegalStateException("unexpected linear scan over expire index: reads=" + reads[0]);
                    }
                    return targetKey[index];
                }
            };

            try {
                Assert.assertTrue(db.pexpire(view, 60_000));
            } catch (IllegalStateException e) {
                Assert.fail(e.getMessage());
            }
        } finally {
            if (dummyKeyAddrs != null) {
                // Make sure the expire index no longer references the dummy off-heap key bytes before we free them.
                // (Future-proof against any shutdown/cleanup path that may iterate/dereference expire index entries.)
                for (int i = 0; i < dummyKeyAddrs.length; i++) {
                    long addr = dummyKeyAddrs[i];
                    if (addr == 0) {
                        continue;
                    }
                    KeyHandle handle = KeyHandle.forOffHeap(allocator, addr, targetKeyLen, i);
                    db.expires.removeExpire(handle);
                }
                for (long addr : dummyKeyAddrs) {
                    if (addr != 0) {
                        allocator.freeAddress(addr, targetKeyLen);
                    }
                }
            }
            db.shutdown();
        }
    }
}
