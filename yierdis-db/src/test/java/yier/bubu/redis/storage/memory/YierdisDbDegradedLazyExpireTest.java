package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.DbAccountingReconciliation;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.lang.reflect.Field;
import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;
import static yier.bubu.redis.storage.testkit.TestBytes.view;

public class YierdisDbDegradedLazyExpireTest {
    private static final String MISCONF_DEGRADED =
            "MISCONF DB is in a degraded state; writes are disabled";

    @Test
    public void writableReadReclaimsAnExpiredKey() {
        YierdisDb db = TestDbSupport.open();
        try {
            byte[] key = b("expired");
            db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
            Assert.assertEquals(1, db.size());

            Assert.assertTrue(OwnedReplyValueAssertions.isNull(db.strings().getStringValue(view(key))));

            Assert.assertEquals(0, db.size());
            Assert.assertNull(db.keyLifecycle().entryRecord(key));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void degradedReadTreatsExpiredKeyAsAbsentWithoutMutating() {
        YierdisDb db = TestDbSupport.open();
        try {
            byte[] expired = b("expired");
            byte[] live = b("live");
            db.strings().setString(expired, b("v"), SetMode.NORMAL, ExpireOption.px(0));
            db.strings().setString(live, b("still-here"), SetMode.NORMAL, null);
            degrade(db);
            Assert.assertEquals(2, db.size());

            Assert.assertTrue(OwnedReplyValueAssertions.isNull(db.strings().getStringValue(view(expired))));
            Assert.assertFalse(db.keyspace().existsKey(view(expired)));
            Assert.assertArrayEquals(b("still-here"), OwnedReplyValueAssertions.stringValue(db.strings(), live));
            Assert.assertNotNull(db.keyLifecycle().entryRecord(expired));
            Assert.assertEquals(2, db.size());

            YierdisCommandException deleted = Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> db.keyspace().del(List.of(expired))
            );
            Assert.assertEquals(MISCONF_DEGRADED, deleted.getMessage());
            YierdisCommandException flushed = Assert.assertThrows(
                    YierdisCommandException.class,
                    db::flushDb
            );
            Assert.assertEquals(MISCONF_DEGRADED, flushed.getMessage());
            YierdisCommandException written = Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> db.strings().setString(b("later"), b("no"), SetMode.NORMAL, null)
            );
            Assert.assertEquals(MISCONF_DEGRADED, written.getMessage());
            Assert.assertNotNull(db.keyLifecycle().entryRecord(expired));
            Assert.assertEquals(2, db.size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void derivedExpireCountUnderflowDegradesUntilReconcileRestoresWrites() throws Exception {
        YierdisDb db = TestDbSupport.open();
        try {
            byte[] key = b("ttl");
            Assert.assertTrue(db.strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(60_000)).value());
            Field expireCount = YierdisDbKeyLifecycle.class.getDeclaredField("expireCount");
            expireCount.setAccessible(true);
            expireCount.setInt(db.keyLifecycle(), 0);

            PostCommitMutationException failure = Assert.assertThrows(
                    PostCommitMutationException.class,
                    () -> db.ttl().persist(view(key))
            );
            Assert.assertEquals("derived expire count underflow", failure.getCause().getMessage());
            Assert.assertTrue(db.health().degraded());
            YierdisCommandException rejected = Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> db.strings().setString(key, b("later"), SetMode.NORMAL, null)
            );
            Assert.assertEquals(MISCONF_DEGRADED, rejected.getMessage());

            DbAccountingReconciliation restored = db.reconcileAccounting();

            Assert.assertTrue(restored.succeeded());
            Assert.assertFalse(db.health().degraded());
            Assert.assertTrue(db.strings().setString(key, b("later"), SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(b("later"), OwnedReplyValueAssertions.stringValue(db.strings(), key));
        } finally {
            db.shutdown();
        }
    }

    private static void degrade(YierdisDb db) {
        db.healthMonitor().recordInvariantFailure(new IllegalStateException("derived expire count underflow"));
    }
}
