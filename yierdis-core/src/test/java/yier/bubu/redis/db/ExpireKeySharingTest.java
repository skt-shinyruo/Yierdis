package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

import static yier.bubu.redis.testutil.TestBytes.b;

public class ExpireKeySharingTest {
    @Test
    public void expireStoresTtlUnderStoreCanonicalKey() throws Exception {
        YierdisDb db = new YierdisDb();
        try {
            byte[] key1 = b("k");
            byte[] key2 = b("k");
            Assert.assertNotSame(key1, key2);

            db.setString(key1, b("v"), YierdisDb.SetMode.NORMAL, null);
            Assert.assertTrue(db.expire(key2, 60));

            ByteArrayKeyspace<?> store = storeKeyspace(db);
            ByteArrayKeyspace<?> expires = expiresKeyspace(db);

            byte[] storeKey = store.canonicalKey(key2);
            Assert.assertSame(key1, storeKey);

            byte[] expiresKey = expires.canonicalKey(key2);
            Assert.assertSame(storeKey, expiresKey);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void expireMigratesPreexistingNonCanonicalExpiresKey() throws Exception {
        YierdisDb db = new YierdisDb();
        try {
            byte[] key1 = b("k");
            byte[] key2 = b("k");
            Assert.assertNotSame(key1, key2);

            db.setString(key1, b("v"), YierdisDb.SetMode.NORMAL, null);

            ByteArrayKeyspace<?> store = storeKeyspace(db);
            ByteArrayKeyspace<Long> expires = expiresKeyspace(db);

            // Simulate an old expires entry inserted under a different byte[] instance.
            expires.compute(key2, (k, old) -> System.currentTimeMillis() + 1234L);
            Assert.assertSame(key2, expires.canonicalKey(key1));

            // Now update TTL through the DB API; it should migrate to the store-canonical key reference.
            Assert.assertTrue(db.expire(key2, 60));

            byte[] canonical = store.canonicalKey(key1);
            Assert.assertSame(key1, canonical);
            Assert.assertSame(canonical, expires.canonicalKey(key1));
        } finally {
            db.shutdown();
        }
    }

    private static ByteArrayKeyspace<?> storeKeyspace(YierdisDb db) throws Exception {
        Field f = YierdisDb.class.getDeclaredField("store");
        f.setAccessible(true);
        return (ByteArrayKeyspace<?>) f.get(db);
    }

    @SuppressWarnings("unchecked")
    private static ByteArrayKeyspace<Long> expiresKeyspace(YierdisDb db) throws Exception {
        Field f = YierdisDb.class.getDeclaredField("expires");
        f.setAccessible(true);
        Object idx = f.get(db);
        if (idx instanceof YierdisHeapExpireIndex heap) {
            return heap.rawKeyspace();
        }
        throw new AssertionError("unexpected expires index type: " + idx.getClass().getName());
    }
}
