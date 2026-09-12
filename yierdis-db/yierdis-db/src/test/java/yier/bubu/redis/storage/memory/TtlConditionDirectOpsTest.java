package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.ExpireCondition;
import yier.bubu.redis.storage.api.SetMode;

import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.testkit.TestBytes.slice;

// 对齐 Redis 7.0+ expireGenericCommand 的条件标志语义（reference/redis/src/expire.c）：
// 无 TTL 的键视为无限 TTL（GT 必然失败、LT 必然成功）；条件先于删除分支判定，
// 条件不满足时键与旧 TTL 都保留，即使新过期时间已经是过去。
public class TtlConditionDirectOpsTest {
    @Test
    public void nxAppliesOnlyWhenKeyHasNoTtl() {
        withDb(db -> {
            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expire(view("k"), 60L, ExpireCondition.NX).value());
            Assert.assertEquals(60L, db.ttl().ttlSeconds(view("k")));

            Assert.assertFalse(db.ttl().expire(view("k"), 120L, ExpireCondition.NX).value());
            Assert.assertEquals(60L, db.ttl().ttlSeconds(view("k")));

            db.strings().setString(b("abs"), b("v"), SetMode.NORMAL, null);
            long futureSeconds = (System.currentTimeMillis() / 1000L) + 60L;
            Assert.assertTrue(db.ttl().expireAtSeconds(view("abs"), futureSeconds, ExpireCondition.NX).value());
            Assert.assertFalse(db.ttl().expireAtSeconds(view("abs"), futureSeconds + 60L, ExpireCondition.NX).value());
            Assert.assertEquals(60L, db.ttl().ttlSeconds(view("abs")));
        });
    }

    @Test
    public void xxAppliesOnlyWhenKeyHasTtl() {
        withDb(db -> {
            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertFalse(db.ttl().expire(view("k"), 60L, ExpireCondition.XX).value());
            Assert.assertEquals(-1L, db.ttl().ttlSeconds(view("k")));

            Assert.assertTrue(db.ttl().expire(view("k"), 60L).value());
            Assert.assertTrue(db.ttl().expire(view("k"), 120L, ExpireCondition.XX).value());
            Assert.assertEquals(120L, db.ttl().ttlSeconds(view("k")));
        });
    }

    @Test
    public void gtRequiresNewExpiryGreaterThanCurrent() {
        withDb(db -> {
            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            // 无 TTL 视为无限 TTL，GT 不可能更大，必然失败且保持无 TTL。
            Assert.assertFalse(db.ttl().expire(view("k"), 60L, ExpireCondition.GT).value());
            Assert.assertEquals(-1L, db.ttl().ttlSeconds(view("k")));

            Assert.assertTrue(db.ttl().expire(view("k"), 60L).value());
            Assert.assertFalse(db.ttl().expire(view("k"), 30L, ExpireCondition.GT).value());
            Assert.assertEquals(60L, db.ttl().ttlSeconds(view("k")));

            Assert.assertTrue(db.ttl().expire(view("k"), 120L, ExpireCondition.GT).value());
            Assert.assertEquals(120L, db.ttl().ttlSeconds(view("k")));

            // 相等也不算 greater；绝对时间消除了相对 TTL 的时钟漂移。
            long deadline = System.currentTimeMillis() + 60_000L;
            Assert.assertTrue(db.ttl().expireAtMillis(view("k"), deadline).value());
            Assert.assertFalse(db.ttl().expireAtMillis(view("k"), deadline, ExpireCondition.GT).value());
        });
    }

    @Test
    public void ltRequiresNewExpiryLessThanCurrent() {
        withDb(db -> {
            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            // 无 TTL 视为无限 TTL，任何有限时间都比它小，LT 必然成功。
            Assert.assertTrue(db.ttl().expire(view("k"), 60L, ExpireCondition.LT).value());
            Assert.assertEquals(60L, db.ttl().ttlSeconds(view("k")));

            Assert.assertFalse(db.ttl().expire(view("k"), 120L, ExpireCondition.LT).value());
            Assert.assertEquals(60L, db.ttl().ttlSeconds(view("k")));

            Assert.assertTrue(db.ttl().expire(view("k"), 30L, ExpireCondition.LT).value());
            Assert.assertEquals(30L, db.ttl().ttlSeconds(view("k")));

            long deadline = System.currentTimeMillis() + 60_000L;
            Assert.assertTrue(db.ttl().expireAtMillis(view("k"), deadline).value());
            Assert.assertFalse(db.ttl().expireAtMillis(view("k"), deadline, ExpireCondition.LT).value());
        });
    }

    @Test
    public void xxCombinedWithGtOrLtFormsIntersection() {
        withDb(db -> {
            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            // XX∧GT 是 Redis 允许的组合：无 TTL 时 XX 子句先失败。
            Assert.assertFalse(db.ttl().expire(view("k"), 60L, ExpireCondition.XX_GT).value());
            Assert.assertEquals(-1L, db.ttl().ttlSeconds(view("k")));

            Assert.assertTrue(db.ttl().expire(view("k"), 60L).value());
            Assert.assertFalse(db.ttl().expire(view("k"), 30L, ExpireCondition.XX_GT).value());
            Assert.assertTrue(db.ttl().expire(view("k"), 120L, ExpireCondition.XX_GT).value());
            Assert.assertEquals(120L, db.ttl().ttlSeconds(view("k")));

            Assert.assertFalse(db.ttl().expire(view("k"), 180L, ExpireCondition.XX_LT).value());
            Assert.assertTrue(db.ttl().expire(view("k"), 90L, ExpireCondition.XX_LT).value());
            Assert.assertEquals(90L, db.ttl().ttlSeconds(view("k")));
        });
    }

    @Test
    public void failedConditionKeepsKeyAndOldTtlOnImmediateDeletePaths() {
        withDb(db -> {
            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expire(view("k"), 60L).value());
            // 过去的新时间 <= 当前 TTL，GT 失败：键保留且不删除。
            Assert.assertFalse(db.ttl().pexpire(view("k"), -1L, ExpireCondition.GT).value());
            Assert.assertNotNull(db.keyspace().typeOf(view("k")));
            Assert.assertEquals(60L, db.ttl().ttlSeconds(view("k")));

            // 过去的新时间必然小于现有 TTL，LT 通过后才真正删除。
            Assert.assertTrue(db.ttl().pexpire(view("k"), -1L, ExpireCondition.LT).value());
            Assert.assertNull(db.keyspace().typeOf(view("k")));

            db.strings().setString(b("p"), b("v"), SetMode.NORMAL, null);
            Assert.assertFalse(db.ttl().pexpire(view("p"), -1L, ExpireCondition.GT).value());
            Assert.assertFalse(db.ttl().pexpire(view("p"), -1L, ExpireCondition.XX).value());
            Assert.assertNotNull(db.keyspace().typeOf(view("p")));
            Assert.assertTrue(db.ttl().pexpire(view("p"), -1L, ExpireCondition.NX).value());
            Assert.assertNull(db.keyspace().typeOf(view("p")));

            db.strings().setString(b("a"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expire(view("a"), 60L).value());
            Assert.assertFalse(db.ttl().expireAtMillis(view("a"), 0L, ExpireCondition.NX).value());
            Assert.assertNotNull(db.keyspace().typeOf(view("a")));
            Assert.assertTrue(db.ttl().expireAtMillis(view("a"), 0L, ExpireCondition.XX).value());
            Assert.assertNull(db.keyspace().typeOf(view("a")));
        });
    }

    @Test
    public void missingKeyNeverAppliesRegardlessOfCondition() {
        withDb(db -> {
            long futureMillis = System.currentTimeMillis() + 60_000L;
            for (ExpireCondition condition : List.of(
                    ExpireCondition.NX, ExpireCondition.XX, ExpireCondition.GT, ExpireCondition.LT)) {
                Assert.assertFalse(db.ttl().expire(view("missing"), 60L, condition).value());
                Assert.assertFalse(db.ttl().expireAtMillis(view("missing"), futureMillis, condition).value());
            }
        });
    }

    @Test
    public void noneConditionPreservesLegacyUnconditionalBehavior() {
        withDb(db -> {
            db.strings().setString(b("k"), b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.ttl().expire(view("k"), 60L, ExpireCondition.NONE).value());
            Assert.assertTrue(db.ttl().pexpire(view("k"), 120_000L, ExpireCondition.NONE).value());
            Assert.assertEquals(120L, db.ttl().ttlSeconds(view("k")));
            Assert.assertTrue(db.ttl().expireAtMillis(view("k"), 0L, ExpireCondition.NONE).value());
            Assert.assertNull(db.keyspace().typeOf(view("k")));
        });
    }

    private static void withDb(DbConsumer consumer) {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            consumer.accept(db);
        } finally {
            db.shutdown();
        }
    }

    private static BytesSlice view(String text) {
        return slice(b(text));
    }

    @FunctionalInterface
    private interface DbConsumer {
        void accept(YierdisDb db);
    }
}
