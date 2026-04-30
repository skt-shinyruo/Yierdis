package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.key.KeyHandleAccess;
import yier.bubu.redis.ops.SetMode;

import java.lang.reflect.Field;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class ExpireKeySharingTest {
    @Test
    public void expireStoresTtlUnderSharedFfmKeyRef() throws Exception {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key1 = b("k");
            byte[] key2 = b("k");
            Assert.assertNotSame(key1, key2);

            db.writes().strings().setString(key1, b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expire(view(key2), 60));

            YierdisKeyspace<?> store = storeKeyspace(db);
            YierdisExpireIndex expires = expiresIndex(db);

            KeyHandle storeHandle = store.keyHandle(key2);
            KeyHandle expireHandle = expires.randomKeyHandle();
            Assert.assertNotNull(storeHandle);
            Assert.assertNotNull(expireHandle);
            Assert.assertSame(KeyHandleAccess.ffmBytesRef(storeHandle), KeyHandleAccess.ffmBytesRef(expireHandle));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void repeatedExpireKeepsSingleSharedFfmKeyRef() throws Exception {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key1 = b("k");
            byte[] key2 = b("k");
            Assert.assertNotSame(key1, key2);

            db.writes().strings().setString(key1, b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expire(view(key2), 60));
            Assert.assertTrue(db.writes().ttl().expire(view(key1), 120));

            YierdisKeyspace<?> store = storeKeyspace(db);
            YierdisExpireIndex expires = expiresIndex(db);
            Assert.assertEquals(1, expires.size());

            KeyHandle storeHandle = store.keyHandle(key1);
            KeyHandle expireHandle = expires.randomKeyHandle();
            Assert.assertNotNull(storeHandle);
            Assert.assertNotNull(expireHandle);
            Assert.assertSame(KeyHandleAccess.ffmBytesRef(storeHandle), KeyHandleAccess.ffmBytesRef(expireHandle));
        } finally {
            db.shutdown();
        }
    }

    private static YierdisKeyspace<?> storeKeyspace(YierdisDb db) throws Exception {
        Field f = YierdisDb.class.getDeclaredField("store");
        f.setAccessible(true);
        return (YierdisKeyspace<?>) f.get(db);
    }

    private static BytesView view(byte[] bytes) {
        return new BytesView() {
            @Override
            public int length() {
                return bytes.length;
            }

            @Override
            public byte getByte(int index) {
                return bytes[index];
            }
        };
    }

    private static YierdisExpireIndex expiresIndex(YierdisDb db) throws Exception {
        Field f = YierdisDb.class.getDeclaredField("expires");
        f.setAccessible(true);
        return (YierdisExpireIndex) f.get(db);
    }
}
