package yier.bubu.redis.memory.foreign;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public class YierdisStableNativeAllocatorTest {
    @Test
    public void allocatesFromPageAllocatorAndRecordsNativeMetadata() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertFalse(handle.isNull());
            Assert.assertEquals(NativeObjectKind.STRING_BYTES.domain(), handle.domain());
            Assert.assertEquals(NativeObjectKind.STRING_BYTES.code(), handle.kindCode());

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                Assert.assertEquals(8, view.size());
                Assert.assertEquals(16, view.capacity());
                view.setByte(0, (byte) 42);
            }

            YierdisNativeObjectMeta meta = allocator.objectMeta(handle, false);
            Assert.assertEquals(handle.slotId(), meta.slotId());
            Assert.assertEquals(handle.generation(), meta.generation());
            Assert.assertEquals(8, meta.size());
            Assert.assertEquals(16, meta.capacity());
            Assert.assertEquals(YierdisNativePageClass.SMALL.ordinal(), meta.pageClass());
            Assert.assertTrue(meta.address() != 0L);

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(8L, stats.logicalUsedBytes());
            Assert.assertEquals(16L, stats.reservedBytes());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, stats.committedBytes());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES - 16L, stats.freeBytes());
            Assert.assertEquals(8L, stats.internalFragmentationBytes());
            Assert.assertEquals(1L, stats.liveObjects());
            Assert.assertEquals(1L, stats.liveSmallPages());
        }
    }

    @Test
    public void detectsUseAfterFree() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            allocator.free(handle);

            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            Assert.assertEquals(1L, allocator.stats().staleHandleDetections());
        }
    }

    @Test
    public void freePinnedObjectQuarantinesUntilUnpin() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            allocator.pin(handle);
            allocator.free(handle);

            NativeAllocatorStats quarantined = allocator.stats();
            Assert.assertEquals(4L, quarantined.logicalUsedBytes());
            Assert.assertEquals(16L, quarantined.reservedBytes());
            Assert.assertEquals(1L, quarantined.pinnedObjects());
            Assert.assertEquals(1L, quarantined.quarantinedObjects());
            Assert.assertEquals(1L, quarantined.liveObjects());
            Assert.assertEquals(
                    YierdisNativeObjectTable.STATE_FREED_QUARANTINED,
                    allocator.objectMeta(handle, true).state()
            );

            try {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
                Assert.fail("expected quarantined object to keep slot unavailable");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }

            allocator.unpin(handle);

            NativeAllocatorStats released = allocator.stats();
            Assert.assertEquals(0L, released.logicalUsedBytes());
            Assert.assertEquals(0L, released.reservedBytes());
            Assert.assertEquals(0L, released.pinnedObjects());
            Assert.assertEquals(0L, released.quarantinedObjects());
            Assert.assertEquals(0L, released.liveObjects());

            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }
        }
    }

    @Test
    public void quarantinedObjectRejectsResolveReallocAndDoubleFreeUntilUnpinned() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            allocator.pin(handle);
            allocator.free(handle);

            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected quarantined resolve rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            try {
                allocator.realloc(handle, 24, NativeReallocPolicy.PRESERVE_PREFIX);
                Assert.fail("expected quarantined realloc rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            try {
                allocator.free(handle);
                Assert.fail("expected quarantined double-free rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            allocator.unpin(handle);

            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle after quarantine release");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }
        }
    }

    @Test
    public void multiplePinsRequireMatchingUnpinsBeforeQuarantineRelease() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            allocator.pin(handle);
            allocator.pin(handle);
            allocator.free(handle);

            allocator.unpin(handle);
            NativeAllocatorStats stillQuarantined = allocator.stats();
            Assert.assertEquals(1L, stillQuarantined.pinnedObjects());
            Assert.assertEquals(1L, stillQuarantined.quarantinedObjects());
            Assert.assertEquals(1L, stillQuarantined.liveObjects());

            allocator.unpin(handle);
            NativeAllocatorStats released = allocator.stats();
            Assert.assertEquals(0L, released.pinnedObjects());
            Assert.assertEquals(0L, released.quarantinedObjects());
            Assert.assertEquals(0L, released.liveObjects());
        }
    }

    @Test
    public void activeEpochDelaysFreedSlotReuseUntilClosed() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle first = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            NativeEpochScope epoch = allocator.beginEpoch(NativeEpochKind.SCAN);

            allocator.free(first);

            NativeAllocatorStats quarantined = allocator.stats();
            Assert.assertEquals(4L, quarantined.logicalUsedBytes());
            Assert.assertEquals(16L, quarantined.reservedBytes());
            Assert.assertEquals(1L, quarantined.quarantinedObjects());
            Assert.assertEquals(1L, quarantined.liveObjects());

            try {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
                Assert.fail("expected active epoch to keep freed slot unavailable");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }

            epoch.close();

            NativeAllocatorStats released = allocator.stats();
            Assert.assertEquals(0L, released.logicalUsedBytes());
            Assert.assertEquals(0L, released.reservedBytes());
            Assert.assertEquals(0L, released.quarantinedObjects());
            Assert.assertEquals(0L, released.liveObjects());

            NativeHandle second = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            Assert.assertEquals(first.slotId(), second.slotId());
            Assert.assertEquals(first.generation() + 1, second.generation());
        }
    }

    @Test
    public void activeSnapshotEpochDelaysFreedSlotReuseUntilClosed() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle first = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            NativeEpochScope epoch = allocator.beginEpoch(NativeEpochKind.SNAPSHOT);

            allocator.free(first);

            NativeAllocatorStats quarantined = allocator.stats();
            Assert.assertEquals(4L, quarantined.logicalUsedBytes());
            Assert.assertEquals(16L, quarantined.reservedBytes());
            Assert.assertEquals(1L, quarantined.quarantinedObjects());
            Assert.assertEquals(1L, quarantined.liveObjects());

            try {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
                Assert.fail("expected active epoch to keep freed slot unavailable");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }

            epoch.close();

            NativeAllocatorStats released = allocator.stats();
            Assert.assertEquals(0L, released.logicalUsedBytes());
            Assert.assertEquals(0L, released.reservedBytes());
            Assert.assertEquals(0L, released.quarantinedObjects());
            Assert.assertEquals(0L, released.liveObjects());

            NativeHandle second = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            Assert.assertEquals(first.slotId(), second.slotId());
            Assert.assertEquals(first.generation() + 1, second.generation());
        }
    }

    @Test
    public void unpinWithoutPinThrows() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);

            try {
                allocator.unpin(handle);
                Assert.fail("expected unpinned rejection");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("not pinned"));
            }
        }
    }

    @Test
    public void reallocPinnedObjectFailsWithoutChangingObject() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 4);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
                view.setByte(3, (byte) 4);
            }

            allocator.pin(handle);

            try {
                allocator.realloc(handle, 24, NativeReallocPolicy.PRESERVE_PREFIX);
                Assert.fail("expected pinned realloc rejection");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("pinned"));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(4L, stats.logicalUsedBytes());
            Assert.assertEquals(1L, stats.liveObjects());
            Assert.assertEquals(1L, stats.pinnedObjects());
            Assert.assertEquals(0L, stats.reallocInPlaceCount());
            Assert.assertEquals(0L, stats.reallocMovedCount());

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(4, view.size());
                Assert.assertEquals(16, view.capacity());
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
                Assert.assertEquals(4, view.getByte(3));
            }

            allocator.unpin(handle);
        }
    }

    @Test
    public void detectsNullHandle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            try {
                allocator.resolve(null, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            Assert.assertEquals(1L, allocator.stats().staleHandleDetections());
        }
    }

    @Test
    public void detectsNativeNullHandle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            try {
                allocator.resolve(NativeHandle.NULL, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            Assert.assertEquals(1L, allocator.stats().staleHandleDetections());
        }
    }

    @Test
    public void oldViewFailsAfterFreeAndSlotReuseAfterClose() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle first = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            NativeObjectView oldView = allocator.resolve(first, NativeAccessMode.READ_WRITE);
            oldView.setByte(0, (byte) 11);

            allocator.free(first);
            try {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
                Assert.fail("expected open resolved view to keep slot quarantined");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }

            try {
                oldView.getByte(0);
                Assert.fail("expected quarantined view");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }
            oldView.close();

            NativeHandle second = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertNotEquals(first.generation(), second.generation());
            try (NativeObjectView newView = allocator.resolve(second, NativeAccessMode.READ_WRITE)) {
                newView.setByte(0, (byte) 22);
            }

            try {
                oldView.getByte(0);
                Assert.fail("expected closed view");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("closed"));
            }

            try (NativeObjectView view = allocator.resolve(second, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(22, view.getByte(0));
            }
        }
    }

    @Test
    public void resolvedViewPinsObjectUntilClosed() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE);
            view.setByte(0, (byte) 11);

            Assert.assertEquals(1L, allocator.stats().pinnedObjects());
            allocator.free(handle);

            NativeAllocatorStats quarantined = allocator.stats();
            Assert.assertEquals(1L, quarantined.pinnedObjects());
            Assert.assertEquals(1L, quarantined.quarantinedObjects());
            Assert.assertEquals(1L, quarantined.liveObjects());

            try {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
                Assert.fail("expected resolved view to keep slot unavailable");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }

            try {
                view.getByte(0);
                Assert.fail("expected quarantined view to reject access");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            view.close();

            NativeAllocatorStats released = allocator.stats();
            Assert.assertEquals(0L, released.pinnedObjects());
            Assert.assertEquals(0L, released.quarantinedObjects());
            Assert.assertEquals(0L, released.liveObjects());

            NativeHandle reused = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertNotEquals(handle.generation(), reused.generation());
        }
    }

    @Test
    public void readOnlyViewRejectsMutation() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                try {
                    view.setByte(0, (byte) 42);
                    Assert.fail("expected read-only rejection");
                } catch (NativeMemoryException expected) {
                    Assert.assertTrue(expected.getMessage().contains("read-only"));
                }
            }
        }
    }

    @Test
    public void rejectsOverflowingViewRanges() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                try {
                    view.getBytes(Integer.MAX_VALUE, new byte[1], 0, 1);
                    Assert.fail("expected range rejection");
                } catch (IndexOutOfBoundsException expected) {
                    Assert.assertNotNull(expected);
                }
            }
        }
    }

    @Test
    public void reallocPreservesHandlePrefixAndUpdatesMetadataWhenMoved() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 16);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
                view.setByte(3, (byte) 4);
            }
            long beforeAddress = allocator.objectMeta(handle, false).address();

            NativeHandle resized = allocator.realloc(handle, 24, NativeReallocPolicy.PRESERVE_PREFIX);
            Assert.assertEquals(handle, resized);

            YierdisNativeObjectMeta after = allocator.objectMeta(handle, false);
            Assert.assertEquals(24, after.size());
            Assert.assertEquals(24, after.capacity());
            Assert.assertNotEquals(beforeAddress, after.address());

            try (NativeObjectView view = allocator.resolve(resized, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(24, view.size());
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
                Assert.assertEquals(4, view.getByte(3));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(24L, stats.logicalUsedBytes());
            Assert.assertEquals(24L, stats.reservedBytes());
            Assert.assertEquals(1L, stats.liveObjects());
            Assert.assertEquals(1L, stats.reallocMovedCount());
        }
    }

    @Test
    public void reallocNoMoveGrowsWithinCapacityAfterShrink() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
                view.setByte(3, (byte) 4);
            }

            NativeHandle shrunk = allocator.realloc(handle, 4, NativeReallocPolicy.NO_MOVE);
            Assert.assertEquals(handle, shrunk);

            NativeHandle grown = allocator.realloc(handle, 6, NativeReallocPolicy.NO_MOVE);
            Assert.assertEquals(handle, grown);

            try (NativeObjectView view = allocator.resolve(grown, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(6, view.size());
                Assert.assertEquals(16, view.capacity());
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
                Assert.assertEquals(4, view.getByte(3));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(6L, stats.logicalUsedBytes());
            Assert.assertEquals(2L, stats.reallocInPlaceCount());
            Assert.assertEquals(0L, stats.reallocMovedCount());
        }
    }

    @Test
    public void reallocNoMoveFailsWithoutChangingObjectWhenGrowthNeedsMove() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 16);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
                view.setByte(3, (byte) 4);
            }

            try {
                allocator.realloc(handle, 24, NativeReallocPolicy.NO_MOVE);
                Assert.fail("expected no-move realloc failure");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("cannot grow in place"));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(16L, stats.logicalUsedBytes());
            Assert.assertEquals(1L, stats.liveObjects());
            Assert.assertEquals(0L, stats.reallocInPlaceCount());
            Assert.assertEquals(0L, stats.reallocMovedCount());

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(16, view.size());
                Assert.assertEquals(16, view.capacity());
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
                Assert.assertEquals(4, view.getByte(3));
            }
        }
    }

    @Test
    public void defragMovesUnpinnedObjectWithoutChangingHandle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
                view.setByte(1, (byte) 2);
                view.setByte(2, (byte) 3);
            }
            long beforeAddress = allocator.objectMeta(handle, false).address();

            NativeDefragResult result = allocator.defragOne(handle, 24);

            Assert.assertTrue(result.moved());
            Assert.assertEquals(24L, result.movedBytes());
            YierdisNativeObjectMeta after = allocator.objectMeta(handle, false);
            Assert.assertEquals(handle.slotId(), after.slotId());
            Assert.assertEquals(handle.generation(), after.generation());
            Assert.assertEquals(24, after.size());
            Assert.assertNotEquals(beforeAddress, after.address());

            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(1, view.getByte(0));
                Assert.assertEquals(2, view.getByte(1));
                Assert.assertEquals(3, view.getByte(2));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(24L, stats.defragMovedBytes());
            Assert.assertEquals(0L, stats.defragSkippedPinnedObjects());
        }
    }

    @Test
    public void activeEpochDelaysDefragOldBlockReleaseUntilClosed() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 1);
            }
            NativeEpochScope epoch = allocator.beginEpoch(NativeEpochKind.DEFRAG);

            NativeDefragResult result = allocator.defragOne(handle, 24);

            Assert.assertTrue(result.moved());
            Assert.assertEquals(48L, allocator.stats().reservedBytes());

            epoch.close();

            Assert.assertEquals(24L, allocator.stats().reservedBytes());
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(1, view.getByte(0));
            }
        }
    }

    @Test
    public void activeSnapshotEpochDelaysDefragOldBlockReleaseUntilClosed() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 7);
            }
            NativeEpochScope epoch = allocator.beginEpoch(NativeEpochKind.SNAPSHOT);

            NativeDefragResult result = allocator.defragOne(handle, 24);

            Assert.assertTrue(result.moved());
            Assert.assertEquals(48L, allocator.stats().reservedBytes());

            epoch.close();

            Assert.assertEquals(24L, allocator.stats().reservedBytes());
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(7, view.getByte(0));
            }
        }
    }

    @Test
    public void defragSkipsPinnedAndOverBudgetObjects() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            long address = allocator.objectMeta(handle, false).address();

            NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY);
            try {
                NativeDefragResult pinned = allocator.defragOne(handle, 24);
                Assert.assertFalse(pinned.moved());
                Assert.assertTrue(pinned.skippedPinned());
                Assert.assertEquals(address, allocator.objectMeta(handle, false).address());
                Assert.assertEquals(1L, allocator.stats().defragSkippedPinnedObjects());
            } finally {
                view.close();
            }

            NativeDefragResult budget = allocator.defragOne(handle, 23);
            Assert.assertFalse(budget.moved());
            Assert.assertTrue(budget.skippedBudget());
            Assert.assertEquals(address, allocator.objectMeta(handle, false).address());
        }
    }

    @Test
    public void defragCycleMovesEligibleObjectsWithinByteBudget() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle first = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            NativeHandle second = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            NativeHandle third = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            long firstAddress = allocator.objectMeta(first, false).address();
            long secondAddress = allocator.objectMeta(second, false).address();
            long thirdAddress = allocator.objectMeta(third, false).address();

            NativeDefragReport report = allocator.defragCycle(new NativeDefragOptions(48, 10, Long.MAX_VALUE));

            Assert.assertEquals(2L, report.movedObjects());
            Assert.assertEquals(48L, report.movedBytes());
            Assert.assertEquals(1L, report.skippedBudgetObjects());
            Assert.assertTrue(report.stoppedByByteBudget());
            Assert.assertNotEquals(firstAddress, allocator.objectMeta(first, false).address());
            Assert.assertNotEquals(secondAddress, allocator.objectMeta(second, false).address());
            Assert.assertEquals(thirdAddress, allocator.objectMeta(third, false).address());
        }
    }

    @Test
    public void defragCycleSkipsPinnedObjectsAndContinues() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle pinned = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            NativeHandle movable = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            long pinnedAddress = allocator.objectMeta(pinned, false).address();
            long movableAddress = allocator.objectMeta(movable, false).address();
            allocator.pin(pinned);

            NativeDefragReport report = allocator.defragCycle(new NativeDefragOptions(48, 10, Long.MAX_VALUE));

            Assert.assertEquals(1L, report.movedObjects());
            Assert.assertEquals(24L, report.movedBytes());
            Assert.assertEquals(1L, report.skippedPinnedObjects());
            Assert.assertEquals(pinnedAddress, allocator.objectMeta(pinned, false).address());
            Assert.assertNotEquals(movableAddress, allocator.objectMeta(movable, false).address());
        }
    }

    @Test
    public void defragValidationFailureRollsBackMove() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(
                     runtime,
                     1024,
                     (handle, sourceMeta, target) -> {
                         throw new NativeMemoryException("validation failed");
                     }
             )) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 7);
            }
            long beforeAddress = allocator.objectMeta(handle, false).address();

            NativeDefragReport report = allocator.defragCycle(new NativeDefragOptions(24, 10, Long.MAX_VALUE));

            Assert.assertEquals(0L, report.movedObjects());
            Assert.assertEquals(1L, report.failedMoves());
            Assert.assertEquals(beforeAddress, allocator.objectMeta(handle, false).address());
            Assert.assertEquals(0L, allocator.stats().defragMovedBytes());
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(7, view.getByte(0));
            }
        }
    }

    @Test
    public void statsExposeProductionAllocatorMetrics() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle string = allocator.allocate(NativeObjectKind.STRING_BYTES, 24);
            NativeHandle entry = allocator.allocate(NativeObjectKind.ENTRY_RECORD, 70_000);

            NativeAllocatorStats allocated = allocator.stats();
            Assert.assertEquals(1L, allocated.objectCount(NativeObjectKind.STRING_BYTES));
            Assert.assertEquals(1L, allocated.objectCount(NativeObjectKind.ENTRY_RECORD));
            Assert.assertTrue(allocated.externalFragmentationBytes() > 0L);
            Assert.assertTrue(allocated.smallFreeBytes() > 0L);
            Assert.assertEquals(0L, allocated.mediumFreeBytes());
            Assert.assertEquals(0L, allocated.largeFreeBytes());
            Assert.assertEquals(0L, allocated.freePages());
            Assert.assertTrue(allocated.allocationLatencyHistogram().allocationCount() >= 2L);

            long reclaimedPages = allocator.objectMeta(entry, false).capacity()
                    / YierdisNativePageAllocator.PAGE_BYTES;
            NativeDefragResult moved = allocator.defragOne(entry, 70_000);
            Assert.assertTrue(moved.moved());
            Assert.assertEquals(reclaimedPages, allocator.stats().defragReclaimedPages());

            allocator.pin(string);
            allocator.free(string);
            Assert.assertTrue(allocator.stats().quarantineBytes() >= 24L);

            try {
                allocator.free(string);
                Assert.fail("expected double-free detection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            Assert.assertEquals(1L, allocator.stats().doubleFreeDetections());
            allocator.unpin(string);
        }
    }

    @Test
    public void deterministicAllocatorChurnStressMaintainsAccounting() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-churn");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 128)) {

            Random random = new Random(0x5eed1234L);
            List<NativeHandle> live = new ArrayList<>();

            for (int i = 0; i < 180; i++) {
                int op = live.isEmpty() ? 0 : random.nextInt(100);
                if (op < 35) {
                    live.add(allocator.allocate(NativeObjectKind.STRING_BYTES, 1 + random.nextInt(128)));
                    continue;
                }

                int index = random.nextInt(live.size());
                NativeHandle handle = live.get(index);
                if (op < 55) {
                    allocator.realloc(handle, 1 + random.nextInt(192), NativeReallocPolicy.PRESERVE_PREFIX);
                } else if (op < 70) {
                    try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                        view.setByte(0, (byte) i);
                        Assert.assertEquals((byte) i, view.getByte(0));
                    }
                } else if (op < 85) {
                    allocator.pin(handle);
                    try {
                        NativeDefragResult result = allocator.defragOne(handle, 192);
                        Assert.assertTrue(result.skippedPinned());
                    } finally {
                        allocator.unpin(handle);
                    }
                } else if (op < 95) {
                    allocator.defragOne(handle, 192);
                } else {
                    allocator.free(handle);
                    live.remove(index);
                    try {
                        allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                        Assert.fail("expected stale handle after churn free");
                    } catch (StaleNativeHandleException expected) {
                        Assert.assertTrue(expected.getMessage().contains("stale native handle"));
                    }
                }
            }

            for (NativeHandle handle : live) {
                allocator.free(handle);
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(0L, stats.logicalUsedBytes());
            Assert.assertEquals(0L, stats.reservedBytes());
            Assert.assertEquals(0L, stats.liveObjects());
            Assert.assertTrue(stats.staleHandleDetections() > 0L);
            Assert.assertTrue(stats.defragSkippedPinnedObjects() > 0L);
            Assert.assertTrue(stats.allocationLatencyHistogram().allocationCount() > 0L);
        }
    }

    @Test
    public void preservePrefixGrowsWithinCapacityAfterShrinkWithoutMove() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1024)) {

            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setByte(0, (byte) 5);
                view.setByte(1, (byte) 6);
                view.setByte(2, (byte) 7);
                view.setByte(3, (byte) 8);
            }

            allocator.realloc(handle, 4, NativeReallocPolicy.PRESERVE_PREFIX);
            NativeHandle grown = allocator.realloc(handle, 6, NativeReallocPolicy.PRESERVE_PREFIX);
            Assert.assertEquals(handle, grown);

            try (NativeObjectView view = allocator.resolve(grown, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(6, view.size());
                Assert.assertEquals(16, view.capacity());
                Assert.assertEquals(5, view.getByte(0));
                Assert.assertEquals(6, view.getByte(1));
                Assert.assertEquals(7, view.getByte(2));
                Assert.assertEquals(8, view.getByte(3));
            }

            NativeAllocatorStats stats = allocator.stats();
            Assert.assertEquals(6L, stats.logicalUsedBytes());
            Assert.assertEquals(2L, stats.reallocInPlaceCount());
            Assert.assertEquals(0L, stats.reallocMovedCount());
        }
    }

    @Test
    public void retiresSlotWhenGenerationSpaceIsExhausted() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 1)) {

            NativeHandle original = allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
            allocator.free(original);

            for (int generation = 2; generation <= 0x0fff; generation++) {
                NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
                Assert.assertEquals(generation, handle.generation());
                allocator.free(handle);
            }

            try {
                allocator.resolve(original, NativeAccessMode.READ_ONLY);
                Assert.fail("expected original handle to remain stale");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            try {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
                Assert.fail("expected retired slot exhaustion");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }
        }
    }

    @Test
    public void closeReleasesAllocatorRuntimeMemory() {
        YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-close");
        YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4);
        try {
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 8);
            Assert.assertTrue(runtime.usedBytes() > 0L);

            allocator.free(handle);
            Assert.assertTrue(runtime.usedBytes() > 0L);

            allocator.close();
            Assert.assertEquals(0L, runtime.usedBytes());

            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected allocator closed");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("closed"));
            }
        } finally {
            allocator.close();
            runtime.close();
        }
    }

    @Test
    public void zeroLengthObjectHasStableHandleAndCanGrowShrinkAndFree() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("stable-zero-length");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 16)) {
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 0);
            try (NativeObjectView initial = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(0, initial.size());
            }

            try (NativeObjectView empty = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(0, empty.size());
                Assert.assertTrue(empty.capacity() > 0);
                empty.getBytes(0, new byte[0], 0, 0);
            }

            NativeHandle grown = allocator.realloc(handle, 3, NativeReallocPolicy.PRESERVE_PREFIX);
            Assert.assertEquals(handle.raw(), grown.raw());
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                view.setBytes(0, new byte[] { 'a', 'b', 'c' }, 0, 3);
            }

            NativeHandle shrunk = allocator.realloc(handle, 0, NativeReallocPolicy.PRESERVE_PREFIX);
            Assert.assertEquals(handle.raw(), shrunk.raw());
            try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(0, view.size());
                view.getBytes(0, new byte[0], 0, 0);
                try {
                    view.getByte(0);
                    Assert.fail("zero-length object should reject byte reads");
                } catch (IndexOutOfBoundsException expected) {
                    // expected
                }
            }

            allocator.free(handle);
            try {
                allocator.resolve(handle, NativeAccessMode.READ_ONLY);
                Assert.fail("expected stale handle after free");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }
        }
    }
}
