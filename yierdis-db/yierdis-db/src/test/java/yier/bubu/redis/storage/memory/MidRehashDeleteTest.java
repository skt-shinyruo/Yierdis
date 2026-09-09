package yier.bubu.redis.storage.memory;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.testkit.TestBytes.view;

public class MidRehashDeleteTest {
    @Test
    public void delAndImmediateTtlDeleteStayCorrectWhileRehashIsInProgress() {
        YierdisDb db = TestDbSupport.openWithNativeSlotCapacity(
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                null,
                65536
        );
        db.bindToCurrentThread();
        try {
            // 回归背景：删除曾靠全表扫描定位槽位；这里持续插入直到目录开始 rehash，让删除落在 old/active 两表并存状态上。
            int inserted = 0;
            while (inserted < 2000 || db.memoryStats().pendingHashTableCount() == 0) {
                Assert.assertTrue("rehash never observed during setup", inserted < 100_000);
                db.strings().setString(b("key-" + inserted), b("v"), SetMode.NORMAL, null);
                inserted++;
            }
            Assert.assertTrue(
                    "setup must leave the directory mid-rehash",
                    db.memoryStats().pendingHashTableCount() != 0
            );

            for (int index = 0; index < inserted; index += 2) {
                Assert.assertEquals(
                        1L,
                        db.keyspace().del(List.of(b("key-" + index))).value().longValue()
                );
            }
            for (int index = 1; index < inserted; index += 2) {
                Assert.assertTrue(db.ttl().expireAtMillis(view(b("key-" + index)), 0L).value());
            }
            Assert.assertEquals(0, db.size());
            for (int index = 0; index < inserted; index++) {
                Assert.assertFalse(db.keyspace().existsKey(view(b("key-" + index))));
            }
            Assert.assertEquals(0L, db.keyspace().del(List.of(b("key-0"))).value().longValue());

            for (int tick = 0; tick < 1000 && db.memoryStats().pendingHashTableCount() != 0; tick++) {
                db.runMaintenance();
            }
            Assert.assertEquals(0, db.memoryStats().pendingHashTableCount());
            Assert.assertEquals(0, db.memoryStats().keyCount());
        } finally {
            db.shutdown();
        }
    }
}
