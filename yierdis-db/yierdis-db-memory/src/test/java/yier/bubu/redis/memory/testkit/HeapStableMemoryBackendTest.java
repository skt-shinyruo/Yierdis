package yier.bubu.redis.memory.testkit;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.StableMemoryRegion;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public class HeapStableMemoryBackendTest {
    @Test
    public void keepsFullHandleIdentityAndOwnerBoundObjectAndRegionState() {
        TestOwner owner = new TestOwner();
        HeapStableMemoryBackend backend = new HeapStableMemoryBackend("heap", 8, owner);
        backend.bindToCurrentThread();

        NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, Long.BYTES);
        Assert.assertTrue(handle.allocatorId() > 0L);
        Assert.assertEquals(1L, handle.localRaw());
        try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_WRITE)) {
            view.setLongLittleEndian(0, 0x0102030405060708L);
        }
        try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_ONLY)) {
            Assert.assertEquals(0x0102030405060708L, view.getLongLittleEndian(0));
        }

        StableMemoryRegion region = backend.allocateRegion("expire-index", Long.BYTES);
        region.setLongLittleEndian(0, 0x1112131415161718L);
        Assert.assertEquals(0x1112131415161718L, region.getLongLittleEndian(0));
        Assert.assertEquals(1L, backend.liveRegionCount());
        region.close();
        Assert.assertEquals(0L, backend.liveRegionCount());

        backend.free(handle);
        backend.close();
    }

    @Test
    public void resolvePinnedRejectsWritableBorrowedViews() {
        TestOwner owner = new TestOwner();
        HeapStableMemoryBackend backend = new HeapStableMemoryBackend("heap", 8, owner);
        backend.bindToCurrentThread();
        NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, 1);
        backend.pin(handle);

        Assert.assertThrows(
                NativeMemoryException.class,
                () -> backend.resolvePinned(handle, NativeAccessMode.READ_WRITE)
        );

        backend.unpin(handle);
        backend.free(handle);
        backend.close();
    }

    @Test
    public void multiByteWritesRejectInvalidRangesBeforeChangingContent() {
        TestOwner owner = new TestOwner();
        HeapStableMemoryBackend backend = new HeapStableMemoryBackend("heap", 8, owner);
        backend.bindToCurrentThread();
        byte[] expected = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, expected.length);

        try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_WRITE)) {
            view.setBytes(0, expected, 0, expected.length);

            Assert.assertThrows(
                    IndexOutOfBoundsException.class,
                    () -> view.setIntLittleEndian(expected.length - 2, 0x11223344)
            );
            assertContentEquals(expected, view);

            Assert.assertThrows(
                    IndexOutOfBoundsException.class,
                    () -> view.setLongLittleEndian(1, 0x0102030405060708L)
            );
            assertContentEquals(expected, view);

            Assert.assertThrows(
                    IndexOutOfBoundsException.class,
                    () -> view.setIntLittleEndian(Integer.MAX_VALUE, 0x11223344)
            );
            assertContentEquals(expected, view);
        }

        backend.free(handle);
        backend.close();
    }

    @Test
    public void freePinnedObjectQuarantinesUntilLastUnpin() {
        TestOwner owner = new TestOwner();
        HeapStableMemoryBackend backend = new HeapStableMemoryBackend("heap", 8, owner);
        backend.bindToCurrentThread();
        NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, 1);
        try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_WRITE)) {
            view.setByte(0, (byte) 42);
        }

        backend.pin(handle);
        backend.pin(handle);
        backend.free(handle);

        Assert.assertThrows(
                StaleNativeHandleException.class,
                () -> backend.resolve(handle, NativeAccessMode.READ_ONLY)
        );
        try (NativeObjectView view = backend.resolvePinned(handle, NativeAccessMode.READ_ONLY)) {
            Assert.assertEquals(42, view.getByte(0));
        }
        Assert.assertEquals(1L, backend.stats().pinnedObjects());
        Assert.assertEquals(1L, backend.stats().quarantinedObjects());

        backend.unpin(handle);
        Assert.assertEquals(1L, backend.stats().pinnedObjects());
        Assert.assertEquals(1L, backend.stats().quarantinedObjects());

        backend.unpin(handle);
        Assert.assertEquals(0L, backend.stats().liveObjects());
        Assert.assertEquals(0L, backend.stats().pinnedObjects());
        Assert.assertEquals(0L, backend.stats().quarantinedObjects());
        Assert.assertThrows(
                StaleNativeHandleException.class,
                () -> backend.resolvePinned(handle, NativeAccessMode.READ_ONLY)
        );
        backend.close();
    }

    private static void assertContentEquals(byte[] expected, NativeObjectView view) {
        byte[] actual = new byte[expected.length];
        view.getBytes(0, actual, 0, actual.length);
        Assert.assertArrayEquals(expected, actual);
    }

    private static final class TestOwner implements MemoryOwner {
        private Thread owner;

        @Override
        public void bindToCurrentThread() {
            Thread current = Thread.currentThread();
            if (owner == null) {
                owner = current;
                return;
            }
            if (owner != current) {
                throw new IllegalStateException("owner is bound to another thread");
            }
        }

        @Override
        public void checkCurrentThread() {
            if (owner != Thread.currentThread()) {
                throw new IllegalStateException("access is outside the owner thread");
            }
        }

        @Override
        public void checkCurrentThreadForShutdown() {
            if (owner != null && owner != Thread.currentThread()) {
                throw new IllegalStateException("shutdown is outside the owner thread");
            }
        }
    }
}
