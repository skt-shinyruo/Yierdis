package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.memory.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.db.memory.ffm.YierdisFfmBytesRef;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.ops.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class OffHeapBytesViewTtlRegressionTest {
    @Test
    public void pexpireBytesViewDoesNotTriggerLinearExpireIndexScan() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, "noeviction", 5, 5, 5);
            byte[] targetKey = b("k00000");
            int targetKeyLen = targetKey.length;
            try {
                db.bindToCurrentThread();
                db.writes().strings().setString(targetKey, new byte[0], SetMode.NORMAL, null);

                int dummyTtlEntries = 8_000;
                long nowMillis = System.currentTimeMillis();
                byte[] dummyKeyBytes = new byte[targetKeyLen];
                dummyKeyBytes[0] = (byte) 'x';
                YierdisFfmBytesRef[] dummyRefs = new YierdisFfmBytesRef[dummyTtlEntries];
                for (int i = 0; i < dummyTtlEntries; i++) {
                    dummyKeyBytes[targetKeyLen - 2] = (byte) (i >>> 8);
                    dummyKeyBytes[targetKeyLen - 1] = (byte) i;
                    dummyRefs[i] = YierdisFfmBlobStore.fromBytes(runtime, dummyKeyBytes);
                    db.expires.setExpireAtMillis(KeyHandle.forFfm(dummyRefs[i], i), nowMillis + 60_000);
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
                    Assert.assertTrue(db.writes().ttl().pexpire(view, 60_000));
                } catch (IllegalStateException e) {
                    Assert.fail(e.getMessage());
                }
            } finally {
                db.shutdown();
            }
        }
    }
}
