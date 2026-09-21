package yier.bubu.redis.storage.memory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class YierdisDbConstructionTest {
    @Test
    public void factoryRejectsNegativeNativeSlotCapacity() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new YierdisDbEngineFactory(HeapStableMemoryBackend::new, -1));
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
                        KeyLifecycleTestAccess.inspect(storage.keyLifecycle()).stableMemoryBackend()
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
    public void partialConstructionCloseAttemptsEveryResourceAndAggregatesFailuresInOrder() {
        try (TestBackend runtime = TestBackend.open("partial-close-failures")) {
            List<String> operations = new ArrayList<>();
            AtomicInteger freeCalls = new AtomicInteger();
            StableMemoryBackend backend = recordingFailureBackend(runtime.backend(), operations, freeCalls);
            EntryTable entries = new EntryTable(backend);
            NativeKeyDirectory directory = new NativeKeyDirectory(backend, HashSeed.random(), new HashTableMaintenanceRegistry());
            StringRoot strings = new StringRoot(backend);
            NativeKeyDirectory.StagedInsert stagedKey = directory.stageInsert(b("k"));
            EntryHandle entryHandle = entries.allocate(entryRecord(
                    stagedKey.keyHandle().nativeHandle()
            ));
            directory.publishStagedInsert(stagedKey, entryHandle);

            IllegalStateException failure = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> YierdisDbKeyLifecycle.closePartiallyConstructed(
                            backend,
                            entries,
                            directory,
                            strings,
                            null,
                            null,
                            null,
                            null
                    )
            );

            Assert.assertEquals("free 1 failed", failure.getMessage());
            Assert.assertEquals(2, failure.getSuppressed().length);
            Assert.assertEquals("free 2 failed", failure.getSuppressed()[0].getMessage());
            Assert.assertEquals("backend close failed", failure.getSuppressed()[1].getMessage());
            Assert.assertEquals(
                    List.of("free-1", "free-2", "backend-close"),
                    operations
            );
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
                16
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
            Assert.assertNotNull(KeyLifecycleTestAccess.backend(db).stats());
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread thread = Thread.ofPlatform().start(() -> {
                try {
                    KeyLifecycleTestAccess.backend(db).stats();
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

    private static EntryRecord entryRecord(yier.bubu.redis.memory.api.NativeHandle keyHandle) {
        return new EntryRecord(
                keyHandle,
                ValueHandle.NULL,
                0,
                yier.bubu.redis.storage.api.ValueType.STRING,
                yier.bubu.redis.storage.memory.internal.value.ValueEncoding.STRING_RAW,
                0,
                -1L,
                1L,
                0L
        );
    }

    private static StableMemoryBackend recordingFailureBackend(
            StableMemoryBackend delegate,
            List<String> operations,
            AtomicInteger freeCalls
    ) {
        return (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[]{StableMemoryBackend.class},
                (ignoredProxy, method, arguments) -> {
                    if (method.getName().equals("free")) {
                        int call = freeCalls.incrementAndGet();
                        operations.add("free-" + call);
                        try {
                            method.invoke(delegate, arguments);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        throw new IllegalStateException("free " + call + " failed");
                    }
                    if (method.getName().equals("close")) {
                        operations.add("backend-close");
                        try {
                            method.invoke(delegate, arguments);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        throw new IllegalStateException("backend close failed");
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                }
        );
    }
}
