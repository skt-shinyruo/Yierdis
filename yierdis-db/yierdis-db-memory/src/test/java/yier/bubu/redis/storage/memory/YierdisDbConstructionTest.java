package yier.bubu.redis.storage.memory;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class YierdisDbConstructionTest {
    @Test
    public void factoryBindsOneOwnerAcrossDatabaseAndBackend() {
        AtomicReference<MemoryOwner> backendOwner = new AtomicReference<>();
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                (name, slots, owner) -> {
                    backendOwner.set(owner);
                    return new HeapStableMemoryBackend(name, slots, owner);
                },
                new YierdisDbBackendConfig(128)
        );
        YierdisDb db = (YierdisDb) factory.create(config(3));
        try {
            Assert.assertSame(db.memoryOwnerForTesting(), backendOwner.get());
            db.bindToCurrentThread();
            Assert.assertTrue(db.writes().strings().setString(b("k"), b("v"), SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(b("v"), db.reads().strings().getStringBytes(b("k")));
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void storageComponentsShareTheProvidedStableBackend() {
        try (TestBackend testBackend = TestBackend.open("shared-components")) {
            YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(
                    testBackend.backend(),
                    HashSeed.random()
            );
            try {
                Assert.assertSame(testBackend.backend(), storage.stableMemoryBackend);
                Assert.assertTrue(storage.stableMemoryBackend.stats().objectCount(NativeObjectKind.ENTRY_RECORD) >= 0L);
            } finally {
                storage.resources.releaseAll(
                        storage.entries,
                        storage.keyDirectory,
                        storage.stringRoot,
                        storage.listRoot,
                        storage.hashRoot,
                        storage.setRoot,
                        storage.zsetRoot
                );
            }
        }
    }

    @Test
    public void configRejectsInvalidValues() {
        Assert.assertThrows(IllegalArgumentException.class, () -> config(-1));
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> new DbEngineConfig(
                        0,
                        0L,
                        MaxmemoryPolicy.NOEVICTION,
                        0,
                        1L,
                        1L,
                        defrag()
                )
        );
    }

    @Test
    public void factoryUsesConfiguredDatabaseIndexInBackendName() {
        AtomicReference<String> name = new AtomicReference<>();
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                (backendName, slots, owner) -> {
                    name.set(backendName);
                    return new HeapStableMemoryBackend(backendName, slots, owner);
                },
                new YierdisDbBackendConfig(16)
        );
        RuntimeDbEngine engine = factory.create(config(7));
        try {
            engine.bindToCurrentThread();
            Assert.assertEquals("db-7", name.get());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    public void testSupportCreatesOwnerBoundDatabase() {
        YierdisDb db = TestDbSupport.open();
        try {
            db.bindToCurrentThread();
            Assert.assertNotNull(db.keyLifecycle().stableMemoryBackend().stats());
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread thread = Thread.ofPlatform().start(() -> {
                try {
                    db.keyLifecycle().stableMemoryBackend().stats();
                } catch (Throwable next) {
                    failure.set(next);
                }
            });
            thread.join();
            Assert.assertTrue(failure.get() instanceof IllegalStateException);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        } finally {
            db.shutdown();
        }
    }

    private static DbEngineConfig config(int dbIndex) {
        return new DbEngineConfig(
                dbIndex,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                defrag()
        );
    }

    private static DbDefragConfig defrag() {
        return new DbDefragConfig(false, 0L, 0L, 0L);
    }
}
