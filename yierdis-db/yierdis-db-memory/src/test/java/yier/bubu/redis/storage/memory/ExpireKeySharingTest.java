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
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class ExpireKeySharingTest {
    @Test
    public void expireStoresTtlUnderSharedFfmKeyRef() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key1 = b("k");
            byte[] key2 = b("k");
            Assert.assertNotSame(key1, key2);

            db.writes().strings().setString(key1, b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expire(view(key2), 60).value());

            KeyHandle storeHandle = db.keyLifecycle().keyHandle(key2);
            KeyHandle expireHandle = db.keyLifecycle().randomExpireKeyHandle();
            Assert.assertNotNull(storeHandle);
            Assert.assertNotNull(expireHandle);
            Assert.assertSame(KeyHandleAccess.ffmBytesRef(storeHandle), KeyHandleAccess.ffmBytesRef(expireHandle));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void repeatedExpireKeepsSingleSharedFfmKeyRef() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key1 = b("k");
            byte[] key2 = b("k");
            Assert.assertNotSame(key1, key2);

            db.writes().strings().setString(key1, b("v"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().expire(view(key2), 60).value());
            Assert.assertTrue(db.writes().ttl().expire(view(key1), 120).value());

            Assert.assertEquals(1, db.memory().memoryStats().expireCount());

            KeyHandle storeHandle = db.keyLifecycle().keyHandle(key1);
            KeyHandle expireHandle = db.keyLifecycle().randomExpireKeyHandle();
            Assert.assertNotNull(storeHandle);
            Assert.assertNotNull(expireHandle);
            Assert.assertSame(KeyHandleAccess.ffmBytesRef(storeHandle), KeyHandleAccess.ffmBytesRef(expireHandle));
        } finally {
            db.shutdown();
        }
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
}
