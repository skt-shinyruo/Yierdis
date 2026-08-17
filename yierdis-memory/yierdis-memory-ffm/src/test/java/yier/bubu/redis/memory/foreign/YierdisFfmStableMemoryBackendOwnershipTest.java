package yier.bubu.redis.memory.foreign;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.*;

public class YierdisFfmStableMemoryBackendOwnershipTest {
    @Test
    public void firstAccessDoesNotBindOwnerImplicitly() {
        FfmTestOwner owner = new FfmTestOwner();
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
        FfmTestOwner firstOwner = new FfmTestOwner();
        FfmTestOwner secondOwner = new FfmTestOwner();
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
        FfmTestOwner owner = new FfmTestOwner();
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
        FfmTestOwner owner = new FfmTestOwner();
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

            NativeEpochScope epochScope = backend.beginEpoch();
            assertWrongThreadRejected(epochScope::close);
            epochScope.close();
        }
    }

    @Test
    public void pinnedViewsAreReadOnlyAndDoNotOwnTheCallersPin() {
        FfmTestOwner owner = new FfmTestOwner();
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
    public void closeReportsAllocatorFailure() {
        FfmTestOwner owner = new FfmTestOwner();
        StableMemoryBackend backend = backend("close-order", owner);
        backend.bindToCurrentThread();
        backend.allocate(NativeObjectKind.GENERIC, 8);
        try {
            IllegalStateException failure = Assert.assertThrows(
                    IllegalStateException.class,
                    backend::close
            );

            Assert.assertTrue(failure.getMessage().contains("live objects"));
            Assert.assertEquals(0, failure.getSuppressed().length);
        } finally {
            backend.close();
        }
    }

    @Test
    public void closeReportsActiveEpochAndStillCleansRuntime() {
        FfmTestOwner owner = new FfmTestOwner();
        StableMemoryBackend backend = backend("active-epoch-close", owner);
        backend.bindToCurrentThread();
        NativeEpochScope epoch = backend.beginEpoch();
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

    private static StableMemoryBackend backend(String name, FfmTestOwner owner) {
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
