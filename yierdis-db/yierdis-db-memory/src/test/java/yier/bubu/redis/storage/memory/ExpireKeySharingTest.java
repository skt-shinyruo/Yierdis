package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;
import yier.bubu.redis.storage.api.SetMode;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class ExpireKeySharingTest {
    @Test
    public void expireStoresTtlUnderSharedAllocatorKeyHandle() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            byte[] key1 = b("k");
            byte[] key2 = b("k");
            Assert.assertNotSame(key1, key2);

            db.writes().strings().setString(key1, b("v"), SetMode.NORMAL, null);
            Assert.assertEquals(1L, db.keyLifecycle().stableMemoryBackend().stats().objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertTrue(db.writes().ttl().expire(view(key2), 60).value());

            KeyHandle storeHandle = db.keyLifecycle().keyHandle(key2);
            KeyHandle expireHandle = db.keyLifecycle().randomExpireKeyHandle();
            Assert.assertNotNull(storeHandle);
            Assert.assertNotNull(expireHandle);
            Assert.assertEquals(1L, db.keyLifecycle().stableMemoryBackend().stats().objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertEquals(allocatorHandle(storeHandle), allocatorHandle(expireHandle));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void repeatedExpireKeepsSingleSharedAllocatorKeyHandle() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            byte[] key1 = b("k");
            byte[] key2 = b("k");
            Assert.assertNotSame(key1, key2);

            db.writes().strings().setString(key1, b("v"), SetMode.NORMAL, null);
            Assert.assertEquals(1L, db.keyLifecycle().stableMemoryBackend().stats().objectCount(NativeObjectKind.KEY_BYTES));
            Assert.assertTrue(db.writes().ttl().expire(view(key2), 60).value());
            Assert.assertTrue(db.writes().ttl().expire(view(key1), 120).value());

            Assert.assertEquals(1, db.memory().memoryStats().expireCount());
            Assert.assertEquals(1L, db.keyLifecycle().stableMemoryBackend().stats().objectCount(NativeObjectKind.KEY_BYTES));

            KeyHandle storeHandle = db.keyLifecycle().keyHandle(key1);
            KeyHandle expireHandle = db.keyLifecycle().randomExpireKeyHandle();
            Assert.assertNotNull(storeHandle);
            Assert.assertNotNull(expireHandle);
            Assert.assertEquals(allocatorHandle(storeHandle), allocatorHandle(expireHandle));
        } finally {
            db.shutdown();
        }
    }

    private static NativeHandle allocatorHandle(KeyHandle handle) {
        NativeHandle nativeHandle = KeyHandleAccess.allocatorNativeHandleOrNull(handle);
        Assert.assertNotNull(nativeHandle);
        return nativeHandle;
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
