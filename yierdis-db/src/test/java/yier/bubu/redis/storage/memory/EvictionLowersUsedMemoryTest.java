package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;

public class EvictionLowersUsedMemoryTest {
    private static final String OOM = "OOM command not allowed when used memory > 'maxmemory'.";

    @Test
    public void allkeysLruWriteSucceedsWhileOtherKeysRemain() {
        int resident = keysBeforeNextWriteOverflows(MaxmemoryPolicy.NOEVICTION);
        Assert.assertTrue("resident=" + resident, resident >= 2);

        try (FfmDb db = open(MaxmemoryPolicy.ALLKEYS_LRU)) {
            for (int i = 0; i < resident; i++) {
                set(db.db(), i);
            }
            set(db.db(), resident);
            Assert.assertTrue(db.db().size() >= 1);
            Assert.assertTrue(db.db().size() < resident + 1);
            Assert.assertTrue(db.db().usedBytesForMaxmemory() <= limitBytes());
        }
    }

    @Test
    public void noevictionStillRejectsTheSameWrite() {
        int resident = keysBeforeNextWriteOverflows(MaxmemoryPolicy.NOEVICTION);
        try (FfmDb db = open(MaxmemoryPolicy.NOEVICTION)) {
            for (int i = 0; i < resident; i++) {
                set(db.db(), i);
            }
            YierdisCommandException failure = Assert.assertThrows(
                    YierdisCommandException.class,
                    () -> set(db.db(), resident)
            );
            Assert.assertEquals(OOM, failure.getMessage());
            Assert.assertEquals(resident, db.db().size());
        }
    }

    private static int keysBeforeNextWriteOverflows(MaxmemoryPolicy policy) {
        try (FfmDb db = open(policy)) {
            int count = 0;
            while (count < 200) {
                try {
                    set(db.db(), count);
                } catch (YierdisCommandException failure) {
                    Assert.assertEquals(OOM, failure.getMessage());
                    return count;
                }
                count++;
            }
            Assert.fail("limit never rejected a small write");
            return count;
        }
    }

    private static void set(YierdisDb db, int index) {
        byte[] key = ("k" + index).getBytes(StandardCharsets.US_ASCII);
        byte[] value = new byte[4_096];
        db.strings().setString(key, value, SetMode.NORMAL, null);
    }

    private static long limitBytes() {
        return 500_000L;
    }

    private static FfmDb open(MaxmemoryPolicy policy) {
        List<YierdisFfmStableMemoryBackend> created = new ArrayList<>(1);
        StableMemoryBackendFactory factory = (name, maxSlots, owner) -> {
            YierdisFfmStableMemoryBackend backend = new YierdisFfmStableMemoryBackend(name, maxSlots, owner);
            created.add(backend);
            return backend;
        };
        YierdisDb db = new YierdisDbEngineFactory(factory, 0).create(new DbEngineConfig(
                0,
                limitBytes(),
                policy,
                5,
                100L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));
        db.bindToCurrentThread();
        Assert.assertEquals(1, created.size());
        return new FfmDb(db);
    }

    private record FfmDb(YierdisDb db) implements AutoCloseable {
        @Override
        public void close() {
            db.shutdown();
        }
    }
}
