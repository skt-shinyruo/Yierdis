package yier.bubu.redis.memory.foreign;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.*;

public class YierdisFfmStableMemoryBackendOwnershipTest {
    @Test
    public void firstAccessDoesNotBindOwnerImplicitly() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("unbound", owner)) {
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> backend.allocate(NativeObjectKind.GENERIC, 8)
            );

            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 8);
            backend.free(handle);
        }
    }

    @Test
    public void sameLocalRawFromTwoBackendsCannotAlias() {
        TestOwner firstOwner = new TestOwner();
        TestOwner secondOwner = new TestOwner();
        try (StableMemoryBackend first = backend("first", firstOwner);
             StableMemoryBackend second = backend("second", secondOwner)) {
            first.bindToCurrentThread();
            second.bindToCurrentThread();
            NativeHandle firstHandle = first.allocate(NativeObjectKind.GENERIC, 8);
            NativeHandle secondHandle = second.allocate(NativeObjectKind.GENERIC, 8);

            Assert.assertEquals(firstHandle.localRaw(), secondHandle.localRaw());
            Assert.assertNotEquals(firstHandle.allocatorId(), secondHandle.allocatorId());
            assertOwnershipFailure(first, secondHandle);

            first.free(firstHandle);
            second.free(secondHandle);
        }
    }

    @Test
    public void ownershipIsCheckedBeforeMalformedLocalRaw() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("order", owner)) {
            backend.bindToCurrentThread();
            NativeHandle foreignMalformed = new NativeHandle(backend.allocatorId() + 1L, Long.MIN_VALUE);

            assertOwnershipFailure(backend, foreignMalformed);
            NativeHandleOwnershipException failure = Assert.assertThrows(
                    NativeHandleOwnershipException.class,
                    () -> backend.resolve(foreignMalformed, NativeAccessMode.READ_ONLY)
            );

            Assert.assertEquals(backend.allocatorId(), failure.expectedAllocatorId());
            Assert.assertEquals(foreignMalformed.allocatorId(), failure.actualAllocatorId());
        }
    }

    @Test
    public void everyDerivedResourceOperationChecksTheBoundOwner() throws Exception {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("derived-owner", owner)) {
            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 16);
            NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_WRITE);

            assertWrongThreadRejected(view::handle);
            assertWrongThreadRejected(view::size);
            assertWrongThreadRejected(view::capacity);
            assertWrongThreadRejected(() -> view.getByte(0));
            assertWrongThreadRejected(() -> view.setByte(0, (byte) 1));
            assertWrongThreadRejected(() -> view.getBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.setBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.copyBytes(0, 1, 1));
            assertWrongThreadRejected(() -> view.contentEquals(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.getIntLittleEndian(0));
            assertWrongThreadRejected(() -> view.setIntLittleEndian(0, 1));
            assertWrongThreadRejected(() -> view.getLongLittleEndian(0));
            assertWrongThreadRejected(() -> view.setLongLittleEndian(0, 1L));
            assertWrongThreadRejected(view::close);
            Assert.assertEquals(16, view.size());
            Assert.assertEquals(0, view.getByte(0));
            view.close();
            backend.free(handle);

            NativeAllocationScope allocationScope = backend.beginAllocationScope();
            assertWrongThreadRejected(allocationScope::growth);
            assertWrongThreadRejected(allocationScope::promote);
            assertWrongThreadRejected(allocationScope::abort);
            assertWrongThreadRejected(allocationScope::close);
            NativeHandle scoped = backend.allocate(NativeObjectKind.GENERIC, 8);
            allocationScope.abort();
            Assert.assertThrows(
                    StaleNativeHandleException.class,
                    () -> backend.resolve(scoped, NativeAccessMode.READ_ONLY)
            );

            NativeEpochScope epochScope = backend.beginEpoch(NativeEpochKind.COMMAND);
            assertWrongThreadRejected(epochScope::kind);
            assertWrongThreadRejected(epochScope::epoch);
            assertWrongThreadRejected(epochScope::close);
            epochScope.close();

            StableMemoryRegion region = backend.allocateRegion("derived-region", 16);
            assertWrongThreadRejected(region::size);
            assertWrongThreadRejected(() -> region.getByte(0));
            assertWrongThreadRejected(() -> region.setByte(0, (byte) 1));
            assertWrongThreadRejected(() -> region.getIntLittleEndian(0));
            assertWrongThreadRejected(() -> region.setIntLittleEndian(0, 1));
            assertWrongThreadRejected(() -> region.getLongLittleEndian(0));
            assertWrongThreadRejected(() -> region.setLongLittleEndian(0, 1L));
            assertWrongThreadRejected(() -> region.getBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> region.setBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> region.copyTo(0, region, 1, 1));
            assertWrongThreadRejected(region::close);
            Assert.assertEquals(16, region.size());
            Assert.assertEquals(0, region.getByte(0));
            region.close();
        }
    }

    @Test
    public void pinnedViewsAreReadOnlyAndDoNotOwnTheCallersPin() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("pinned-view", owner)) {
            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 8);
            backend.pin(handle);

            Assert.assertThrows(
                    NativeMemoryException.class,
                    () -> backend.resolvePinned(handle, NativeAccessMode.READ_WRITE)
            );
            try (NativeObjectView borrowed =
                         backend.resolvePinned(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(handle, borrowed.handle());
                Assert.assertThrows(
                        NativeMemoryException.class,
                        () -> borrowed.setByte(0, (byte) 1)
                );
            }
            try (NativeObjectView ignored =
                         backend.resolvePinned(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(handle, ignored.handle());
            }

            backend.unpin(handle);
            backend.free(handle);
        }
    }

    @Test
    public void regionProvidesBackendNeutralTypedAccessAndCopy() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("regions", owner)) {
            backend.bindToCurrentThread();
            try (StableMemoryRegion source = backend.allocateRegion("source", 32);
                 StableMemoryRegion target = backend.allocateRegion("target", 32)) {
                source.setByte(0, (byte) 7);
                source.setIntLittleEndian(4, 0x78563412);
                source.setLongLittleEndian(8, 0x0102030405060708L);
                byte[] sourceBytes = new byte[12];
                source.getBytes(4, sourceBytes, 0, sourceBytes.length);
                Assert.assertArrayEquals(
                        new byte[]{
                                0x12, 0x34, 0x56, 0x78,
                                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01
                        },
                        sourceBytes
                );
                source.copyTo(0, target, 0, 16);

                Assert.assertEquals(7, target.getByte(0));
                Assert.assertEquals(0x78563412, target.getIntLittleEndian(4));
                Assert.assertEquals(0x0102030405060708L, target.getLongLittleEndian(8));
                byte[] targetBytes = new byte[12];
                target.getBytes(4, targetBytes, 0, targetBytes.length);
                Assert.assertArrayEquals(sourceBytes, targetBytes);
                Assert.assertTrue(backend.liveRegionCount() >= 2L);
            }
        }
    }

    @Test
    public void sameRegionCopyIsOverlapSafeAcrossPortableChunkBoundary() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("overlap", owner)) {
            backend.bindToCurrentThread();
            try (StableMemoryRegion region = backend.allocateRegion("overlap", 16_384)) {
                byte[] original = new byte[16_384];
                for (int index = 0; index < original.length; index++) {
                    original[index] = (byte) (index % 251);
                }
                region.setBytes(0, original, 0, original.length);

                region.copyTo(0, region, 4_096, 12_000);

                byte[] expected = original.clone();
                System.arraycopy(original, 0, expected, 4_096, 12_000);
                byte[] actual = new byte[expected.length];
                region.getBytes(0, actual, 0, actual.length);
                Assert.assertArrayEquals(expected, actual);
            }
        }
    }

    @Test
    public void regionRejectsUseAfterClose() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("closed-region", owner)) {
            backend.bindToCurrentThread();
            StableMemoryRegion region = backend.allocateRegion("closed", 16);
            region.close();

            Assert.assertThrows(IllegalStateException.class, region::size);
            Assert.assertThrows(IllegalStateException.class, () -> region.getByte(0));
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> region.setIntLittleEndian(0, 1)
            );
        }
    }

    @Test
    public void externallyAllocatedRegionIsCountedOnce() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("accounting", owner)) {
            backend.bindToCurrentThread();
            long before = backend.memoryUsage().nativeDataCommittedBytes();
            try (StableMemoryRegion ignored = backend.allocateRegion("index", 257)) {
                Assert.assertEquals(
                        257L,
                        backend.memoryUsage().nativeDataCommittedBytes() - before
                );
            }
            Assert.assertEquals(before, backend.memoryUsage().nativeDataCommittedBytes());
        }
    }

    @Test
    public void closeReportsAllocatorFailureBeforeRuntimeLeak() {
        TestOwner owner = new TestOwner();
        StableMemoryBackend backend = backend("close-order", owner);
        backend.bindToCurrentThread();
        backend.allocate(NativeObjectKind.GENERIC, 8);
        StableMemoryRegion region = backend.allocateRegion("live-region", 32);
        try {
            IllegalStateException failure = Assert.assertThrows(
                    IllegalStateException.class,
                    backend::close
            );

            Assert.assertTrue(failure.getMessage().contains("live objects"));
            Assert.assertEquals(1, failure.getSuppressed().length);
            Assert.assertTrue(failure.getSuppressed()[0].getMessage().contains("live regions"));
        } finally {
            region.close();
            backend.close();
        }
    }

    @Test
    public void closeReportsActiveEpochAndStillCleansRuntime() {
        TestOwner owner = new TestOwner();
        StableMemoryBackend backend = backend("active-epoch-close", owner);
        backend.bindToCurrentThread();
        NativeEpochScope epoch = backend.beginEpoch(NativeEpochKind.SNAPSHOT);
        try {
            IllegalStateException failure = Assert.assertThrows(
                    IllegalStateException.class,
                    backend::close
            );

            Assert.assertTrue(failure.getMessage().contains("active epochs"));
            Assert.assertEquals(0L, backend.liveRegionCount());
        } finally {
            epoch.close();
            backend.close();
        }
    }

    private static void assertOwnershipFailure(StableMemoryBackend backend, NativeHandle foreign) {
        Assert.assertThrows(
                NativeHandleOwnershipException.class,
                () -> backend.reallocate(foreign, 16, NativeReallocPolicy.PRESERVE_PREFIX)
        );
        Assert.assertThrows(
                NativeHandleOwnershipException.class,
                () -> backend.resolve(foreign, NativeAccessMode.READ_ONLY)
        );
        Assert.assertThrows(
                NativeHandleOwnershipException.class,
                () -> backend.resolvePinned(foreign, NativeAccessMode.READ_ONLY)
        );
        Assert.assertThrows(NativeHandleOwnershipException.class, () -> backend.free(foreign));
        Assert.assertThrows(NativeHandleOwnershipException.class, () -> backend.pin(foreign));
        Assert.assertThrows(NativeHandleOwnershipException.class, () -> backend.unpin(foreign));
        Assert.assertThrows(
                NativeHandleOwnershipException.class,
                () -> backend.defragOne(foreign, 16L)
        );
    }

    private static StableMemoryBackend backend(String name, TestOwner owner) {
        return new YierdisFfmStableMemoryBackend(name, 128, owner);
    }

    private static void assertWrongThreadRejected(Runnable operation) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                operation.run();
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "wrong-memory-owner");
        worker.setDaemon(true);
        worker.start();
        worker.join(5_000L);

        if (worker.isAlive()) worker.interrupt();
        Assert.assertFalse("wrong-owner operation did not finish", worker.isAlive());
        Assert.assertTrue(
                "expected owner rejection, got " + failure.get(),
                failure.get() instanceof IllegalStateException
        );
    }

    private static final class TestOwner implements MemoryOwner {
        private final AtomicReference<Thread> owner = new AtomicReference<>();

        @Override
        public void bindToCurrentThread() {
            Thread current = Thread.currentThread();
            Thread existing = owner.get();
            if (existing == current) return;
            if (existing != null || !owner.compareAndSet(null, current)) {
                throw new IllegalStateException("memory owner already belongs to another thread");
            }
        }

        @Override
        public void checkCurrentThread() {
            if (owner.get() != Thread.currentThread()) {
                throw new IllegalStateException("memory access is outside the owner thread");
            }
        }

        @Override
        public void checkCurrentThreadForShutdown() {
            Thread existing = owner.get();
            if (existing != null && existing != Thread.currentThread()) {
                throw new IllegalStateException("memory shutdown is outside the owner thread");
            }
        }
    }
}

final class FfmTestOwner implements MemoryOwner {
    private final AtomicReference<Thread> owner = new AtomicReference<>();

    @Override
    public void bindToCurrentThread() {
        Thread current = Thread.currentThread();
        Thread existing = owner.get();
        if (existing == current) {
            return;
        }
        if (existing != null || !owner.compareAndSet(null, current)) {
            throw new IllegalStateException("memory owner already belongs to another thread");
        }
    }

    @Override
    public void checkCurrentThread() {
        if (owner.get() != Thread.currentThread()) {
            throw new IllegalStateException("memory access is outside the owner thread");
        }
    }

    @Override
    public void checkCurrentThreadForShutdown() {
        Thread existing = owner.get();
        if (existing != null && existing != Thread.currentThread()) {
            throw new IllegalStateException("memory shutdown is outside the owner thread");
        }
    }
}
