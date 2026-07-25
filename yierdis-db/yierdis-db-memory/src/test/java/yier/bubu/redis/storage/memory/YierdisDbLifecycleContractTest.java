package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocatorMetadataStats;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StableMemoryRegion;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.memory.internal.expire.YierdisNativeExpireIndex;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

public class YierdisDbLifecycleContractTest {
    @Test
    public void unboundDatabaseCanShutdownWithoutInitializingExpiryStorage() {
        YierdisDb engine = (YierdisDb) new YierdisDbEngineFactory(
                HeapStableMemoryBackend::new,
                new YierdisDbBackendConfig(64)
        ).create(TestDbSupport.config());

        engine.shutdown();
        engine.shutdown();
    }

    @Test
    public void failedShutdownClosesPublicAccessAndDoesNotReleaseResourcesTwice() {
        AtomicReference<YierdisDb> engineReference = new AtomicReference<>();
        AtomicReference<CallbackAndFailingCloseBackend> backendReference = new AtomicReference<>();
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                (name, slots, owner) -> {
                    CallbackAndFailingCloseBackend backend = new CallbackAndFailingCloseBackend(
                            new HeapStableMemoryBackend(name, slots, owner),
                            () -> {
                                YierdisDb engine = engineReference.get();
                                Assert.assertNotNull("database must be available during backend close", engine);
                                IllegalStateException failure = Assert.assertThrows(
                                        IllegalStateException.class,
                                        engine::runMaintenance
                                );
                                Assert.assertTrue(failure.getMessage().contains("CLOSING"));
                            }
                    );
                    backendReference.set(backend);
                    return backend;
                },
                new YierdisDbBackendConfig(64)
        );
        YierdisDb engine = (YierdisDb) factory.create(TestDbSupport.config());
        engineReference.set(engine);
        engine.bindToCurrentThread();

        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                engine::shutdown
        );

        Assert.assertEquals("injected backend close failure", failure.getMessage());
        IllegalStateException closedFailure = Assert.assertThrows(
                IllegalStateException.class,
                engine::runMaintenance
        );
        Assert.assertTrue(closedFailure.getMessage().contains("CLOSED"));
        Assert.assertEquals(1, backendReference.get().closeCalls());

        engine.shutdown();

        Assert.assertEquals(1, backendReference.get().closeCalls());
    }

    @Test
    public void failedExpiryIndexCloseDoesNotRecreateItsRegionsDuringShutdown() {
        DbThreadGuard owner = new DbThreadGuard();
        HeapStableMemoryBackend delegate = new HeapStableMemoryBackend("failed-expiry-close", 64, owner);
        AtomicBoolean failFirstRegionClose = new AtomicBoolean(true);
        AtomicInteger regionAllocations = new AtomicInteger();
        List<StableMemoryRegion> allocatedRegions = new ArrayList<>();
        StableMemoryBackend backend = closeFailingRegionBackend(
                delegate,
                failFirstRegionClose,
                regionAllocations,
                allocatedRegions
        );
        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(backend);
        NativeHandle key = NativeHandle.NULL;
        try {
            backend.bindToCurrentThread();
            key = backend.allocate(NativeObjectKind.KEY_BYTES, 1);
            YierdisNativeExpireIndex index = new YierdisNativeExpireIndex(
                    backend,
                    new HashSeed(1L, 2L),
                    null
            );
            index.setExpireAtMillis(KeyHandle.forNative(backend, key, 0), 1L);
            int allocationsBeforeShutdown = regionAllocations.get();

            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> resources.releaseAll(index, null, null, null, null, null, null, null)
            );

            Assert.assertEquals(
                    "shutdown must not rebuild the expiry index after a close failure",
                    allocationsBeforeShutdown,
                    regionAllocations.get()
            );
        } finally {
            for (StableMemoryRegion region : allocatedRegions) {
                try {
                    region.close();
                } catch (RuntimeException ignored) {
                    // 回归用例故意注入一次 close 失败，测试清理不能覆盖断言结果。
                }
            }
            if (!key.isNull()) {
                backend.free(key);
            }
            delegate.close();
        }
    }

    private static StableMemoryBackend closeFailingRegionBackend(
            StableMemoryBackend delegate,
            AtomicBoolean failFirstRegionClose,
            AtomicInteger regionAllocations,
            List<StableMemoryRegion> allocatedRegions
    ) {
        return (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[]{StableMemoryBackend.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    if (method.getName().equals("allocateRegion")) {
                        regionAllocations.incrementAndGet();
                        StableMemoryRegion region = (StableMemoryRegion) invoke(delegate, method, arguments);
                        allocatedRegions.add(region);
                        return closeFailingRegion(region, failFirstRegionClose);
                    }
                    return invoke(delegate, method, arguments);
                }
        );
    }

    private static StableMemoryRegion closeFailingRegion(
            StableMemoryRegion delegate,
            AtomicBoolean failFirstRegionClose
    ) {
        return (StableMemoryRegion) Proxy.newProxyInstance(
                StableMemoryRegion.class.getClassLoader(),
                new Class<?>[]{StableMemoryRegion.class},
                (proxy, method, arguments) -> {
                    Object result = invoke(delegate, method, arguments);
                    if (method.getName().equals("close")
                            && failFirstRegionClose.compareAndSet(true, false)) {
                        throw new IllegalStateException("injected expiry-region close failure");
                    }
                    return result;
                }
        );
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] arguments)
            throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static final class CallbackAndFailingCloseBackend implements StableMemoryBackend {
        private final StableMemoryBackend delegate;
        private final Runnable closeCallback;
        private boolean failFirstClose = true;
        private int closeCalls;

        private CallbackAndFailingCloseBackend(StableMemoryBackend delegate, Runnable closeCallback) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.closeCallback = Objects.requireNonNull(closeCallback, "closeCallback");
        }

        int closeCalls() {
            return closeCalls;
        }

        @Override
        public long allocatorId() {
            return delegate.allocatorId();
        }

        @Override
        public void bindToCurrentThread() {
            delegate.bindToCurrentThread();
        }

        @Override
        public NativeHandle allocate(NativeObjectKind kind, int size) {
            return delegate.allocate(kind, size);
        }

        @Override
        public NativeHandle reallocate(
                NativeHandle handle,
                int newSize,
                NativeReallocPolicy policy
        ) {
            return delegate.reallocate(handle, newSize, policy);
        }

        @Override
        public void free(NativeHandle handle) {
            delegate.free(handle);
        }

        @Override
        public void pin(NativeHandle handle) {
            delegate.pin(handle);
        }

        @Override
        public void unpin(NativeHandle handle) {
            delegate.unpin(handle);
        }

        @Override
        public NativeEpochScope beginEpoch(NativeEpochKind kind) {
            return delegate.beginEpoch(kind);
        }

        @Override
        public NativeAllocationScope beginAllocationScope() {
            return delegate.beginAllocationScope();
        }

        @Override
        public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
            return delegate.estimateAllocationScopeBookkeepingBytes(expectedAllocationCount);
        }

        @Override
        public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
            return delegate.resolve(handle, mode);
        }

        @Override
        public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
            return delegate.resolvePinned(handle, mode);
        }

        @Override
        public StableMemoryRegion allocateRegion(String owner, int bytes) {
            return delegate.allocateRegion(owner, bytes);
        }

        @Override
        public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
            return delegate.defragOne(handle, maxMoveBytes);
        }

        @Override
        public NativeDefragReport defragCycle(NativeDefragOptions options) {
            return delegate.defragCycle(options);
        }

        @Override
        public long logicalUsedBytes() {
            return delegate.logicalUsedBytes();
        }

        @Override
        public NativeAllocatorStats stats() {
            return delegate.stats();
        }

        @Override
        public NativeAllocatorMetadataStats metadataStats() {
            return delegate.metadataStats();
        }

        @Override
        public MemoryUsageSnapshot memoryUsage() {
            return delegate.memoryUsage();
        }

        @Override
        public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
            return delegate.trimEmptyPages(budget);
        }

        @Override
        public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
            return delegate.estimateAdditionalGrowth(requestedBytes);
        }

        @Override
        public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
            return delegate.estimateConservativeAdditionalGrowth(requestedBytes);
        }

        @Override
        public long liveRegionCount() {
            return delegate.liveRegionCount();
        }

        @Override
        public void close() {
            closeCalls++;
            closeCallback.run();
            delegate.close();
            if (failFirstClose) {
                failFirstClose = false;
                throw new IllegalStateException("injected backend close failure");
            }
        }
    }
}
