package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class ExpireIndexTest {
    @Test
    public void cleanupExpiredRemovesImmediatelyExpiredKeysWithoutAccess() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
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
        db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
        db.writes().strings().setString(key, b("v2"), SetMode.NORMAL, null);

        db.cleanupExpired();

        Assert.assertEquals(1, db.size());
        Assert.assertArrayEquals(b("v2"), db.reads().strings().getStringBytes(key));

        db.shutdown();
    }

    @Test
    public void cleanupExpiredEventuallyRemovesManyExpiredKeysWithoutAccess() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        int n = 200;
        for (int i = 0; i < n; i++) {
            byte[] key = b("k" + i);
            db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(0));
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
        db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(60_000));

        db.cleanupExpired();

        Assert.assertEquals(1, db.size());
        Assert.assertArrayEquals(b("v"), db.reads().strings().getStringBytes(key));

        db.shutdown();
    }

    @Test
    public void ttlBytesViewLazilyDeletesExpiredKeys() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.writes().strings().setString(key, b("v"), SetMode.NORMAL, null);
        Assert.assertEquals(1, db.size());

        long nowMillis = System.currentTimeMillis();
        db.setExpireAtMillis(key, nowMillis - 1);

        BytesView view = viewOf(key);

        Assert.assertEquals(-2L, db.reads().ttl().ttlSeconds(view));
        Assert.assertEquals(0, db.size());

        db.shutdown();
    }

    @Test
    public void ttlAccountingAffectsUsedBytesForMaxmemory() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.writes().strings().setString(key, b("v"), SetMode.NORMAL, null);
        long usedBeforeTtl = db.usedBytesForMaxmemory();
        BytesView keyView = viewOf(key);

        Assert.assertTrue(db.writes().ttl().expire(keyView, 60).value());
        long usedAfterTtl = db.usedBytesForMaxmemory();
        Assert.assertTrue(usedAfterTtl >= usedBeforeTtl + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE);

        // Updating TTL should not add more metadata entries.
        Assert.assertTrue(db.writes().ttl().expire(keyView, 120).value());
        Assert.assertEquals(usedAfterTtl, db.usedBytesForMaxmemory());

        Assert.assertTrue(db.writes().ttl().persist(keyView).value());
        long usedAfterPersist = db.usedBytesForMaxmemory();
        Assert.assertEquals(0, db.memory().memoryStats().expireCount());
        Assert.assertTrue(usedAfterPersist >= usedBeforeTtl);
        Assert.assertTrue(usedAfterPersist <= usedAfterTtl - DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE);

        db.shutdown();
    }

    @Test
    public void cleanupExpiredNowMillisHonorsArgument() {
        YierdisDb db = new YierdisDb();
        db.bindToCurrentThread();

        byte[] key = b("k");
        db.writes().strings().setString(key, b("v"), SetMode.NORMAL, ExpireOption.px(60_000));
        Assert.assertEquals(1, db.size());

        long now = System.currentTimeMillis();
        long farFuture = now + 120_000L;
        db.cleanupExpired(farFuture);

        Assert.assertEquals(0, db.size());
        db.shutdown();
    }

    private static BytesView viewOf(byte[] data) {
        return new BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }
}
