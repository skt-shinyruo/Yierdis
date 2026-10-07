package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

public class FlushDbMetadataReclaimTest {
    private static final int KEYS = 2_048;

    @Test
    public void syncFlushReturnsUsedMemoryAndMetadataToTheEmptyBaseline() {
        try (FfmDb flushed = openFfmDb(0);
             FfmDb other = openFfmDb(1)) {
            long flushedUsed = flushed.db().usedBytesForMaxmemory();
            long flushedMetadata = flushed.db().memoryUsage().nativeMetadataCommittedBytes();
            long otherUsed = other.db().usedBytesForMaxmemory();
            fill(other.db(), 1);
            long otherUsedAfterWrite = other.db().usedBytesForMaxmemory();
            Assert.assertTrue(otherUsedAfterWrite > otherUsed);

            fill(flushed.db(), KEYS);
            Assert.assertTrue(flushed.db().memoryUsage().nativeMetadataCommittedBytes() > flushedMetadata);
            Assert.assertTrue(flushed.db().usedBytesForMaxmemory() > flushedUsed);

            flushed.db().flushDb();

            Assert.assertEquals(0, flushed.db().size());
            Assert.assertEquals(flushedMetadata, flushed.db().memoryUsage().nativeMetadataCommittedBytes());
            Assert.assertEquals(flushedUsed, flushed.db().usedBytesForMaxmemory());
            Assert.assertEquals(otherUsedAfterWrite, other.db().usedBytesForMaxmemory());
            Assert.assertEquals(1, other.db().size());
        }
    }

    @Test
    public void asyncFlushReturnsMetadataInOneReclamationPass() {
        try (FfmDb db = openFfmDb(0)) {
            MemoryUsageSnapshot empty = db.db().memoryUsage();
            fill(db.db(), KEYS);
            Assert.assertTrue(db.db().memoryUsage().nativeMetadataCommittedBytes() > empty.nativeMetadataCommittedBytes());

            db.db().flushDbAsync();
            Assert.assertEquals(0, db.db().size());
            Assert.assertTrue(db.db().usedBytesForMaxmemory() > empty.effectiveBytesForMaxmemory());

            db.db().runDeferredReclamation();

            Assert.assertEquals(0L, KeyLifecycleTestAccess.inspect(db.db().keyLifecycle()).keyDirectory().detachedEntryCount());
            Assert.assertEquals(empty, db.db().memoryUsage());
        }
    }

    private static void fill(YierdisDb db, int keyCount) {
        for (int i = 0; i < keyCount; i++) {
            db.strings().setString(
                    ("k" + i).getBytes(StandardCharsets.UTF_8),
                    ("v" + i).getBytes(StandardCharsets.UTF_8),
                    SetMode.NORMAL,
                    null
            );
        }
    }

    private static FfmDb openFfmDb(int dbIndex) {
        List<YierdisFfmStableMemoryBackend> created = new ArrayList<>(1);
        StableMemoryBackendFactory factory = (name, maxSlots, owner) -> {
            YierdisFfmStableMemoryBackend backend = new YierdisFfmStableMemoryBackend(name, maxSlots, owner);
            created.add(backend);
            return backend;
        };
        YierdisDb db = new YierdisDbEngineFactory(factory, 0).create(new DbEngineConfig(
                dbIndex,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                0L,
                0L,
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
