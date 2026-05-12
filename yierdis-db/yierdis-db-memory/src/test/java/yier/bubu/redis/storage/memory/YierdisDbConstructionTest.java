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
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.Collections;
import java.nio.charset.StandardCharsets;

public class YierdisDbConstructionTest {
    @Test
    public void nullAndBlankMaxmemoryPoliciesDefaultToNoeviction() {
        assertConstructsWithPolicy(null);
        assertConstructsWithPolicy("");
        assertConstructsWithPolicy("   ");
    }

    @Test
    public void typedConfigDefaultsNullPolicyToNoeviction() {
        YierdisDbConfig config = YierdisDbConfig.create(0, null, 5, 5, 5);
        Assert.assertSame(MaxmemoryPolicy.NOEVICTION, config.maxmemoryPolicy);
    }

    @Test
    public void policyParsingNormalizesCaseAndUnderscore() {
        assertConstructsWithPolicy("ALLKEYS_RANDOM");
        assertConstructsWithPolicy("allkeys_LRU");
        assertConstructsWithPolicy("  NoEviction  ");
    }

    @Test
    public void typedConfigComputesLruEnabledFromCorePolicy() {
        YierdisDbConfig lru = YierdisDbConfig.create(1, MaxmemoryPolicy.ALLKEYS_LRU, 5, 5, 5);
        Assert.assertTrue(lru.lruEnabled);

        YierdisDbConfig noLimit = YierdisDbConfig.create(0, MaxmemoryPolicy.ALLKEYS_LRU, 5, 5, 5);
        Assert.assertFalse(noLimit.lruEnabled);

        YierdisDbConfig random = YierdisDbConfig.create(1, MaxmemoryPolicy.ALLKEYS_RANDOM, 5, 5, 5);
        Assert.assertFalse(random.lruEnabled);
    }

    @Test
    public void unknownPolicyStillThrowsIllegalArgumentException() {
        try {
            new YierdisDb((OffHeapAllocator) null, 0, "unknown-policy", 5, 5, 5);
            Assert.fail("unknown policy should fail construction");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("unknown maxmemory policy"));
        }
    }

    @Test
    public void invalidConstructionNumbersStillThrowIllegalArgumentException() {
        assertInvalid(-1, "noeviction", 5, 5, 5, "maxmemoryBytes");
        assertInvalid(0, "noeviction", 0, 5, 5, "maxmemorySamples");
        assertInvalid(0, "noeviction", 5, 0, 5, "evictionTimeLimitMillis");
        assertInvalid(0, "noeviction", 5, 5, 0, "expireCleanupTimeLimitMillis");
    }

    @Test
    public void storageComponentsCarryNativeEntryDirectoryGraph() {
        YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(null, null, false, false);
        Assert.assertNotNull(storage.entries);
        Assert.assertNotNull(storage.keyDirectory);
        storage.resources.releaseAll(
                storage.expires,
                storage.entries,
                storage.keyDirectory,
                storage.stringRoot,
                storage.listRoot,
                storage.hashRoot,
                storage.setRoot,
                storage.zsetRoot
        );
    }

    @Test
    public void shutdownReleasesNativeEntryDirectoryResources() {
        YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-entry-graph-test");
        YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
        db.bindToCurrentThread();
        EntryHandle handle = db.keyLifecycle().entryTable().allocate(new EntryRecord(
                1L,
                new ValueHandle(2L),
                3,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                0,
                -1L,
                0L,
                0L
        ));
        db.keyLifecycle().keyDirectory().compute("native-key".getBytes(StandardCharsets.UTF_8), (key, old) -> handle);
        Assert.assertTrue(runtime.usedBytes() > 0L);

        db.shutdown();

        Assert.assertEquals(0L, runtime.usedBytes());
        runtime.close();
    }

    @Test
    public void normalWritesUpdateNativeEntryDirectoryMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-meta");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);

            EntryHandle handle = db.keyLifecycle().entryHandle(key);
            Assert.assertNotNull(handle);
            EntryRecord record = db.keyLifecycle().entryRecord(handle);
            Assert.assertNotNull(record);
            Assert.assertEquals(ValueType.STRING, record.type());
            Assert.assertEquals(ValueEncoding.STRING_EMBSTR, record.encoding());
            Assert.assertEquals(-1L, record.expireAtMillis());
            Assert.assertTrue(record.version() > 0L);
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void ttlAndDeleteUpdateNativeEntryDirectoryMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-ttl");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);
            Assert.assertTrue(db.writes().ttl().pexpire(view(key), 60_000L).value());

            EntryRecord ttlRecord = db.keyLifecycle().entryRecord(key);
            Assert.assertNotNull(ttlRecord);
            Assert.assertTrue(ttlRecord.expireAtMillis() > System.currentTimeMillis());

            Assert.assertEquals(1L, (long) db.writes().keyspace().del(Collections.singletonList(key)).value());
            Assert.assertNull(db.keyLifecycle().entryHandle(key));
            Assert.assertNull(db.keyLifecycle().entryRecord(key));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void activeExpirationCleanupUnlinksNativeEntryMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-expire-cleanup");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);
            Assert.assertNotNull(db.keyLifecycle().entryHandle(key));
            Assert.assertEquals(1, db.keyLifecycle().entryTable().size());
            Assert.assertTrue(db.writes().ttl().pexpire(view(key), 1L).value());

            db.cleanupExpired(System.currentTimeMillis() + 1_000L);

            Assert.assertNull(db.keyLifecycle().entryHandle(key));
            Assert.assertEquals(0, db.keyLifecycle().entryTable().size());
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void evictionUnlinksNativeEntryMetadata() {
        YierdisDb db = new YierdisDb();
        try {
            db.bindToCurrentThread();
            byte[] key = bytes("native-evict");

            db.writes().strings().setString(key, bytes("value"), SetMode.NORMAL, null);
            Assert.assertNotNull(db.keyLifecycle().entryHandle(key));
            Assert.assertEquals(1, db.keyLifecycle().entryTable().size());

            MaxmemoryCandidate candidate = db.sampleCandidate(MaxmemoryPolicy.ALLKEYS_RANDOM, System.currentTimeMillis());
            Assert.assertNotNull(candidate);
            Assert.assertTrue(db.evict(candidate, System.currentTimeMillis()));

            Assert.assertNull(db.keyLifecycle().entryHandle(key));
            Assert.assertEquals(0, db.keyLifecycle().entryTable().size());
        } finally {
            db.shutdown();
        }
    }

    private static void assertConstructsWithPolicy(String policy) {
        YierdisDb db = new YierdisDb((OffHeapAllocator) null, 0, policy, 5, 5, 5);
        try {
            db.bindToCurrentThread();
        } finally {
            db.shutdown();
        }
    }

    private static void assertInvalid(
            long maxmemoryBytes,
            String policy,
            int samples,
            long evictionMillis,
            long expireMillis,
            String messagePart
    ) {
        try {
            new YierdisDb((OffHeapAllocator) null, maxmemoryBytes, policy, samples, evictionMillis, expireMillis);
            Assert.fail("invalid construction should fail: " + messagePart);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains(messagePart));
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static yier.bubu.redis.bytes.BytesView view(byte[] data) {
        return new yier.bubu.redis.bytes.BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                if (index < 0 || index >= data.length) {
                    throw new IndexOutOfBoundsException();
                }
                return data[index];
            }
        };
    }
}
