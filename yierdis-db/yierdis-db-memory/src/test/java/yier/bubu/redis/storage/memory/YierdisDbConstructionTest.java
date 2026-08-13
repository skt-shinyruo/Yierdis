package yier.bubu.redis.storage.memory;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
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
    public void storageOwnershipUsesTheProvidedStableBackend() {
        try (TestBackend testBackend = TestBackend.open("shared-components")) {
            YierdisDbStorage storage = YierdisDbStorage.create(
                    testBackend.backend(),
                    HashSeed.random(),
                    () -> 0L
            );
            try {
                Assert.assertSame(
                        testBackend.backend(),
                        storage.keyLifecycle().inspectionForTesting().stableMemoryBackend()
                );
                Assert.assertTrue(
                        storage.keyLifecycle().inspectionForTesting().stableMemoryBackend()
                                .stats().objectCount(NativeObjectKind.ENTRY_RECORD) >= 0L
                );
            } finally {
                storage.close();
            }
        }
    }

    @Test
    public void constructionPreservesPrimaryFailureAndClosesBackendOnce() {
        AtomicInteger closeCalls = new AtomicInteger();
        var backend = (yier.bubu.redis.memory.api.StableMemoryBackend) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{yier.bubu.redis.memory.api.StableMemoryBackend.class},
                (ignoredProxy, method, ignoredArguments) -> {
                    if (method.getName().equals("close")) {
                        closeCalls.incrementAndGet();
                        throw new IllegalStateException("injected construction cleanup failure");
                    }
                    throw new AssertionError("unexpected backend call during failed construction: " + method.getName());
                }
        );

        NullPointerException failure = Assert.assertThrows(
                NullPointerException.class,
                () -> YierdisDb.create(null, backend, new DbThreadGuard(), HashSeed.random())
        );

        Assert.assertEquals("config", failure.getMessage());
        Assert.assertEquals(1, closeCalls.get());
        Assert.assertEquals(1, failure.getSuppressed().length);
        Assert.assertEquals("injected construction cleanup failure", failure.getSuppressed()[0].getMessage());
    }

    @Test
    public void storageValidationFailureConsumesAndClosesBackendOwnership() {
        AtomicInteger closeCalls = new AtomicInteger();
        var backend = (yier.bubu.redis.memory.api.StableMemoryBackend) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{yier.bubu.redis.memory.api.StableMemoryBackend.class},
                (ignoredProxy, method, ignoredArguments) -> {
                    if (method.getName().equals("close")) {
                        closeCalls.incrementAndGet();
                        return null;
                    }
                    throw new AssertionError("unexpected backend call during validation: " + method.getName());
                }
        );

        NullPointerException failure = Assert.assertThrows(
                NullPointerException.class,
                () -> YierdisDbStorage.create(backend, null, () -> 0L)
        );

        Assert.assertEquals("hashSeed", failure.getMessage());
        Assert.assertEquals(1, closeCalls.get());
        Assert.assertEquals(0, failure.getSuppressed().length);
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
            Assert.assertNotNull(db.stableMemoryBackend().stats());
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread thread = Thread.ofPlatform().start(() -> {
                try {
                    db.stableMemoryBackend().stats();
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
