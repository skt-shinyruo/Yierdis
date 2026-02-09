package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;

import static yier.bubu.redis.testutil.TestBytes.b;

public class ExpireIndexTest {
    @Test
    public void cleanupExpiredRemovesImmediatelyExpiredKeysWithoutAccess() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        Assert.assertEquals(1, db.size());

        db.cleanupExpired();
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void staleExpireIndexEntriesDoNotDeleteKeysWhenTtlIsCleared() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        db.setString(key, b("v2"), SetMode.NORMAL, null);

        db.cleanupExpired();

        Assert.assertEquals(1, db.size());
        Assert.assertArrayEquals(b("v2"), db.getStringBytes(key));

        db.shutdown();
    }

    @Test
    public void cleanupExpiredEventuallyRemovesManyExpiredKeysWithoutAccess() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        int n = 200;
        for (int i = 0; i < n; i++) {
            byte[] key = b("k" + i);
        db.setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        }
        Assert.assertEquals(n, db.size());

        for (int i = 0; i < 100 && db.size() > 0; i++) {
            db.cleanupExpired();
        }
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void cleanupExpiredDoesNotDeleteUnexpiredKeys() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(60_000));

        db.cleanupExpired();

        Assert.assertEquals(1, db.size());
        Assert.assertArrayEquals(b("v"), db.getStringBytes(key));

        db.shutdown();
    }
}
