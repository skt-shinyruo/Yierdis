package yier.bubu.redis.memory.foreign;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

public class SynchronizedStableMemoryBackendTest {
    @Test
    public void exposesOnlyBackendNeutralConstructor() throws Exception {
        Assert.assertEquals(1, SynchronizedStableMemoryBackend.class.getConstructors().length);
        Assert.assertNotNull(
                SynchronizedStableMemoryBackend.class.getConstructor(StableMemoryBackend.class)
        );
        Assert.assertThrows(
                NoSuchMethodException.class,
                () -> SynchronizedStableMemoryBackend.class.getConstructor(
                        String.class,
                        int.class,
                        MemoryOwner.class
                )
        );
    }

    @Test
    public void twoThreadsCanAllocateAndFreeThroughAdapter() throws Exception {
        try (SynchronizedStableMemoryBackend backend = newBackend("synchronized-adapter", 128)) {
            backend.bindToCurrentThread();
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread first = worker(backend, start, failure);
            Thread second = worker(backend, start, failure);

            start.countDown();
            first.join();
            second.join();

            if (failure.get() != null) {
                throw new AssertionError("concurrent allocator access failed", failure.get());
            }
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void foreignThreadCannotTerminallyCloseOwnersAllocationScope() throws Exception {
        try (SynchronizedStableMemoryBackend backend = newBackend("synchronized-scope-owner", 128)) {
            backend.bindToCurrentThread();
            NativeAllocationScope scope = backend.beginAllocationScope();
            backend.allocate(NativeObjectKind.STRING_BYTES, 32);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread foreign = Thread.ofPlatform().start(() -> {
                try {
                    scope.abort();
                } catch (Throwable expected) {
                    failure.set(expected);
                }
            });
            foreign.join();

            Assert.assertTrue(failure.get() instanceof IllegalStateException);
            scope.abort();
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void foreignThreadCannotCloseBackendWithOwnersAllocationScope() throws Exception {
        try (SynchronizedStableMemoryBackend backend = newBackend("synchronized-close-owner", 128)) {
            backend.bindToCurrentThread();
            NativeAllocationScope scope = backend.beginAllocationScope();
            backend.allocate(NativeObjectKind.STRING_BYTES, 32);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread foreign = Thread.ofPlatform().start(() -> {
                try {
                    backend.close();
                } catch (Throwable expected) {
                    failure.set(expected);
                }
            });
            foreign.join();

            Assert.assertTrue(failure.get() instanceof IllegalStateException);
            scope.abort();
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void crossWrapperRegionCopyRetainsTargetLock() throws Exception {
        ProbeRegion sourceDelegateRegion = new ProbeRegion(false);
        ProbeRegion targetDelegateRegion = new ProbeRegion(true);
        try (SynchronizedStableMemoryBackend source =
                     new SynchronizedStableMemoryBackend(new ProbeBackend(1L, sourceDelegateRegion));
             SynchronizedStableMemoryBackend target =
                     new SynchronizedStableMemoryBackend(new ProbeBackend(2L, targetDelegateRegion))) {
            StableMemoryRegion sourceRegion = source.allocateRegion("source", 16);
            StableMemoryRegion targetRegion = target.allocateRegion("target", 16);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread copy = Thread.ofPlatform().start(() -> {
                try {
                    sourceRegion.copyTo(0, targetRegion, 0, 1);
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            Assert.assertTrue(targetDelegateRegion.writeStarted.await(5, TimeUnit.SECONDS));

            Thread mutate = Thread.ofPlatform().start(() -> {
                try {
                    targetRegion.setByte(0, (byte) 1);
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });

            Assert.assertFalse(targetDelegateRegion.singleByteEntered.await(200, TimeUnit.MILLISECONDS));
            targetDelegateRegion.releaseWrite.countDown();
            copy.join(5_000L);
            mutate.join(5_000L);

            Assert.assertFalse(copy.isAlive());
            Assert.assertFalse(mutate.isAlive());
            Assert.assertNull(failure.get());
            Assert.assertTrue(targetDelegateRegion.singleByteEntered.await(5, TimeUnit.SECONDS));
            sourceRegion.close();
            targetRegion.close();
        }
    }

    @Test
    public void completedScopeCannotClearLaterScopesOwnership() throws Exception {
        try (SynchronizedStableMemoryBackend backend = newBackend("synchronized-scope-generation", 128)) {
            backend.bindToCurrentThread();
            NativeAllocationScope completed = backend.beginAllocationScope();
            completed.abort();
            CountDownLatch opened = new CountDownLatch(1);
            CountDownLatch finish = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread owner = Thread.ofPlatform().start(() -> {
                try (NativeAllocationScope ignored = backend.beginAllocationScope()) {
                    opened.countDown();
                    finish.await();
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            opened.await();

            completed.abort();
            completed.promote();
            completed.close();
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> backend.allocate(NativeObjectKind.STRING_BYTES, 32)
            );

            finish.countDown();
            owner.join();
            if (failure.get() != null) {
                throw new AssertionError("later scope owner failed", failure.get());
            }
        }
    }

    private static Thread worker(
            SynchronizedStableMemoryBackend backend,
            CountDownLatch start,
            AtomicReference<Throwable> failure
    ) {
        return Thread.ofPlatform().start(() -> {
            try {
                start.await();
                for (int i = 0; i < 100; i++) {
                    NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, 32);
                    backend.free(handle);
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });
    }

    private static SynchronizedStableMemoryBackend newBackend(String name, int maxSlots) {
        return new SynchronizedStableMemoryBackend(
                new YierdisFfmStableMemoryBackend(name, maxSlots, new SynchronizedTestOwner())
        );
    }

    private static final class SynchronizedTestOwner implements MemoryOwner {
        @Override
        public void bindToCurrentThread() {
        }

        @Override
        public void checkCurrentThread() {
        }

        @Override
        public void checkCurrentThreadForShutdown() {
        }
    }

    private static final class ProbeBackend implements StableMemoryBackend {
        private final long id;
        private final StableMemoryRegion region;

        private ProbeBackend(long id, StableMemoryRegion region) {
            this.id = id;
            this.region = region;
        }

        @Override public long allocatorId() { return id; }
        @Override public void bindToCurrentThread() { }
        @Override public NativeHandle allocate(NativeObjectKind kind, int size) { throw unsupported(); }
        @Override public NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy) { throw unsupported(); }
        @Override public void free(NativeHandle handle) { throw unsupported(); }
        @Override public void pin(NativeHandle handle) { throw unsupported(); }
        @Override public void unpin(NativeHandle handle) { throw unsupported(); }
        @Override public NativeEpochScope beginEpoch(NativeEpochKind kind) { throw unsupported(); }
        @Override public NativeAllocationScope beginAllocationScope() { throw unsupported(); }
        @Override public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) { throw unsupported(); }
        @Override public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) { throw unsupported(); }
        @Override public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) { throw unsupported(); }
        @Override public StableMemoryRegion allocateRegion(String owner, int bytes) { return region; }
        @Override public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) { throw unsupported(); }
        @Override public NativeDefragReport defragCycle(NativeDefragOptions options) { throw unsupported(); }
        @Override public long logicalUsedBytes() { throw unsupported(); }
        @Override public NativeAllocatorStats stats() { throw unsupported(); }
        @Override public NativeAllocatorMetadataStats metadataStats() { throw unsupported(); }
        @Override public MemoryUsageSnapshot memoryUsage() { throw unsupported(); }
        @Override public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) { throw unsupported(); }
        @Override public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) { throw unsupported(); }
        @Override public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) { throw unsupported(); }
        @Override public long liveRegionCount() { throw unsupported(); }
        @Override public void close() { }

        private static UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException();
        }
    }

    private static final class ProbeRegion implements StableMemoryRegion {
        private final boolean blockWrites;
        private final CountDownLatch writeStarted = new CountDownLatch(1);
        private final CountDownLatch releaseWrite = new CountDownLatch(1);
        private final CountDownLatch singleByteEntered = new CountDownLatch(1);

        private ProbeRegion(boolean blockWrites) {
            this.blockWrites = blockWrites;
        }

        @Override public int size() { return 16; }
        @Override public byte getByte(int offset) { return 0; }
        @Override public void setByte(int offset, byte value) { singleByteEntered.countDown(); }
        @Override public int getIntLittleEndian(int offset) { return 0; }
        @Override public void setIntLittleEndian(int offset, int value) { }
        @Override public long getLongLittleEndian(int offset) { return 0L; }
        @Override public void setLongLittleEndian(int offset, long value) { }
        @Override public void getBytes(int offset, byte[] dst, int dstOffset, int length) { }

        @Override
        public void setBytes(int offset, byte[] src, int srcOffset, int length) {
            if (!blockWrites) {
                return;
            }
            writeStarted.countDown();
            try {
                if (!releaseWrite.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting to release target write");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        }

        @Override
        public void copyTo(int sourceOffset, StableMemoryRegion target, int targetOffset, int length) {
            target.setBytes(targetOffset, new byte[length], 0, length);
        }

        @Override public void close() { }
    }
}
