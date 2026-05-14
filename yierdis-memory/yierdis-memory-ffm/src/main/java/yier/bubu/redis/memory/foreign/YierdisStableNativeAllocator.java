package yier.bubu.redis.memory.foreign;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public final class YierdisStableNativeAllocator implements NativeAllocator {
    private static final int REALLOC_COPY_CHUNK_BYTES = 64 * 1024;

    private final YierdisNativePageAllocator pageAllocator;
    private final YierdisNativeObjectTable objectTable;
    private final YierdisNativeEpochManager epochManager = new YierdisNativeEpochManager();
    private final Map<Long, Allocation> allocations = new HashMap<>();
    private final List<RetainedBlock> retainedMovedBlocks = new ArrayList<>();

    private boolean closed;
    private long logicalUsedBytes;
    private long reservedBytes;
    private long liveObjects;
    private long staleHandleDetections;
    private long reallocInPlaceCount;
    private long reallocMovedCount;
    private long defragMovedBytes;
    private long defragSkippedPinnedObjects;

    public YierdisStableNativeAllocator(YierdisFfmMemoryRuntime runtime, int maxSlots) {
        Objects.requireNonNull(runtime, "runtime");
        this.pageAllocator = new YierdisNativePageAllocator(runtime);
        this.objectTable = new YierdisNativeObjectTable(runtime, maxSlots, 0);
    }

    @Override
    public synchronized NativeHandle allocate(NativeObjectKind kind, int size) {
        ensureOpen();
        Objects.requireNonNull(kind, "kind");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }

        YierdisNativeBlock block = pageAllocator.allocate(size);
        boolean allocated = false;
        try {
            NativeHandle handle = objectTable.allocate(
                    kind,
                    size,
                    block.capacity(),
                    packedAddress(block),
                    block.pageClass().ordinal(),
                    epochManager.nextEpoch()
            );
            Allocation allocation = new Allocation(block);
            allocation.lastHandle = handle;
            allocations.put(handle.slotId(), allocation);
            logicalUsedBytes += size;
            reservedBytes += block.capacity();
            liveObjects++;
            allocated = true;
            return handle;
        } finally {
            if (!allocated) {
                block.close();
            }
        }
    }

    @Override
    public synchronized NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        ensureOpen();
        Objects.requireNonNull(policy, "policy");
        if (newSize <= 0) {
            throw new IllegalArgumentException("newSize must be > 0");
        }

        YierdisNativeObjectMeta meta = requireLiveMeta(handle);
        if (meta.pinCount() > 0) {
            throw new NativeMemoryException("native object is pinned");
        }

        Allocation allocation = allocationFor(handle);
        int oldSize = meta.size();
        if (newSize <= allocation.current.capacity()) {
            objectTable.updateLocation(
                    handle,
                    newSize,
                    allocation.current.capacity(),
                    packedAddress(allocation.current),
                    allocation.current.pageClass().ordinal()
            );
            logicalUsedBytes += (long) newSize - oldSize;
            reallocInPlaceCount++;
            return handle;
        }

        if (policy == NativeReallocPolicy.NO_MOVE) {
            throw new NativeMemoryException("native object cannot grow in place");
        }

        YierdisNativeBlock next = pageAllocator.allocate(newSize);
        boolean moved = false;
        try {
            copyPrefix(allocation.current, next, oldSize);
            YierdisNativeBlock previous = allocation.current;
            objectTable.updateLocation(
                    handle,
                    newSize,
                    next.capacity(),
                    packedAddress(next),
                    next.pageClass().ordinal()
            );
            allocation.current = next;
            logicalUsedBytes += (long) newSize - oldSize;
            reserveMovedBlock(previous, next.capacity(), epochManager.nextEpoch());
            reallocMovedCount++;
            moved = true;
            return handle;
        } finally {
            if (!moved) {
                next.close();
            }
        }
    }

    @Override
    public synchronized void free(NativeHandle handle) {
        ensureOpen();
        YierdisNativeObjectMeta meta = requireLiveMeta(handle);
        Allocation allocation = allocationFor(handle);
        long freeEpoch = epochManager.nextEpoch();
        boolean delayRelease = meta.pinCount() > 0 || !epochManager.canReclaim(freeEpoch);
        objectTable.free(handle, freeEpoch, delayRelease);
        if (delayRelease) {
            allocation.quarantined = true;
            allocation.retiredEpoch = freeEpoch;
            reclaimEligibleQuarantine();
            return;
        }
        releaseAllocation(handle.slotId(), allocation, meta.size());
    }

    @Override
    public synchronized void pin(NativeHandle handle) {
        ensureOpen();
        trackStale(() -> {
            objectTable.pin(trackStale(handle));
            return objectTable.snapshot(handle, false);
        });
    }

    @Override
    public synchronized void unpin(NativeHandle handle) {
        ensureOpen();
        YierdisNativeObjectMeta before = objectMeta(handle, true);
        Allocation allocation = allocationFor(handle);
        objectTable.unpin(trackStale(handle), false);
        if (before.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED && before.pinCount() == 1) {
            allocation.retiredEpoch = before.freeEpoch();
            reclaimEligibleQuarantine();
        }
    }

    @Override
    public synchronized NativeEpochScope beginEpoch(NativeEpochKind kind) {
        ensureOpen();
        NativeEpochScope delegate = epochManager.begin(kind);
        return new AllocatorEpochScope(delegate);
    }

    @Override
    public synchronized NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        ensureOpen();
        Objects.requireNonNull(mode, "mode");
        requireLiveMeta(handle);
        Allocation allocation = allocationFor(handle);
        objectTable.pin(trackStale(handle));
        return new StableObjectView(handle, allocation, mode);
    }

    @Override
    public synchronized NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        ensureOpen();
        if (maxMoveBytes < 0) {
            throw new IllegalArgumentException("maxMoveBytes must be >= 0");
        }
        YierdisNativeObjectMeta meta = requireLiveMeta(handle);
        if (meta.size() > maxMoveBytes) {
            return NativeDefragResult.skippedMoveBudget();
        }
        if (meta.pinCount() > 0) {
            defragSkippedPinnedObjects++;
            return NativeDefragResult.skippedPinnedObject();
        }

        Allocation allocation = allocationFor(handle);
        YierdisNativeBlock target = pageAllocator.allocate(meta.size());
        boolean moved = false;
        try {
            copyPrefix(allocation.current, target, meta.size());
            YierdisNativeBlock previous = allocation.current;
            objectTable.updateLocation(
                    handle,
                    meta.size(),
                    target.capacity(),
                    packedAddress(target),
                    target.pageClass().ordinal()
            );
            allocation.current = target;
            reserveMovedBlock(previous, target.capacity(), epochManager.nextEpoch());
            defragMovedBytes += meta.size();
            moved = true;
            return NativeDefragResult.moved(meta.size());
        } finally {
            if (!moved) {
                target.close();
            }
        }
    }

    @Override
    public synchronized NativeAllocatorStats stats() {
        YierdisNativePageAllocatorStats pageStats = pageAllocator.stats();
        return new NativeAllocatorStats(
                logicalUsedBytes,
                reservedBytes,
                pageStats.committedBytes(),
                pageStats.freeBytes(),
                reservedBytes - logicalUsedBytes,
                pageStats.liveSmallPages(),
                pageStats.liveMediumSpanPages(),
                pageStats.liveLargeSpanPages(),
                liveObjects,
                pinnedObjects(),
                quarantinedObjects(),
                staleHandleDetections,
                reallocInPlaceCount,
                reallocMovedCount,
                defragMovedBytes,
                defragSkippedPinnedObjects
        );
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;

        RuntimeException failure = null;
        for (Allocation allocation : allocations.values()) {
            failure = closeBlock(allocation.current, failure);
        }
        for (RetainedBlock retained : retainedMovedBlocks) {
            failure = closeBlock(retained.block, failure);
        }
        allocations.clear();
        retainedMovedBlocks.clear();
        logicalUsedBytes = 0L;
        reservedBytes = 0L;
        liveObjects = 0L;

        try {
            objectTable.close();
        } catch (RuntimeException e) {
            failure = addFailure(failure, e);
        }

        try {
            pageAllocator.close();
        } catch (RuntimeException e) {
            failure = addFailure(failure, e);
        }

        if (failure != null) {
            throw failure;
        }
    }

    synchronized YierdisNativeObjectMeta objectMeta(NativeHandle handle, boolean allowQuarantined) {
        ensureOpen();
        return trackStale(() -> objectTable.snapshot(handle, allowQuarantined));
    }

    private YierdisNativeObjectMeta requireLiveMeta(NativeHandle handle) {
        return trackStale(() -> objectTable.resolve(handle));
    }

    private NativeHandle trackStale(NativeHandle handle) {
        try {
            if (handle == null || handle.isNull()) {
                throw new StaleNativeHandleException("stale native handle: null");
            }
            return handle;
        } catch (StaleNativeHandleException e) {
            staleHandleDetections++;
            throw e;
        }
    }

    private YierdisNativeObjectMeta trackStale(MetaSupplier supplier) {
        try {
            return supplier.get();
        } catch (StaleNativeHandleException e) {
            staleHandleDetections++;
            throw e;
        }
    }

    private Allocation allocationFor(NativeHandle handle) {
        Allocation allocation = allocations.get(trackStale(handle).slotId());
        if (allocation == null) {
            staleHandleDetections++;
            throw new StaleNativeHandleException("stale native handle: missing allocation " + handle.slotId());
        }
        return allocation;
    }

    private void releaseAllocation(long slotId, Allocation allocation, int logicalSize) {
        if (allocations.remove(slotId) == null) {
            staleHandleDetections++;
            throw new StaleNativeHandleException("stale native handle: missing allocation " + slotId);
        }
        logicalUsedBytes -= logicalSize;
        reservedBytes -= allocation.current.capacity();
        liveObjects--;
        allocation.current.close();
    }

    private void releaseQuarantinedAllocation(long slotId, Allocation allocation, int logicalSize) {
        objectTable.releaseQuarantined(allocation.lastHandle);
        releaseAllocation(slotId, allocation, logicalSize);
    }

    private void reserveMovedBlock(YierdisNativeBlock block, int nextCapacity, long retiredEpoch) {
        if (epochManager.canReclaim(retiredEpoch)) {
            block.close();
            reservedBytes += (long) nextCapacity - block.capacity();
            return;
        }
        for (Allocation allocation : allocations.values()) {
            if (allocation.current == block) {
                throw new IllegalStateException("cannot quarantine current native block");
            }
        }
        reservedBytes += nextCapacity;
        retainedMovedBlocks.add(new RetainedBlock(block, retiredEpoch));
    }

    private void reclaimEligibleQuarantine() {
        reclaimEligibleFreedObjects();
        reclaimEligibleMovedBlocks();
    }

    private void reclaimEligibleFreedObjects() {
        Long[] slotIds = allocations.keySet().toArray(Long[]::new);
        for (Long slotId : slotIds) {
            Allocation allocation = allocations.get(slotId);
            if (allocation == null || !allocation.quarantined || allocation.retiredEpoch <= 0) {
                continue;
            }
            if (!epochManager.canReclaim(allocation.retiredEpoch)) {
                continue;
            }
            YierdisNativeObjectMeta meta = objectTable.snapshot(allocation.lastHandle, true);
            if (meta.pinCount() != 0) {
                continue;
            }
            releaseQuarantinedAllocation(slotId, allocation, meta.size());
        }
    }

    private void reclaimEligibleMovedBlocks() {
        for (int i = 0; i < retainedMovedBlocks.size(); ) {
            RetainedBlock retained = retainedMovedBlocks.get(i);
            if (!epochManager.canReclaim(retained.retiredEpoch)) {
                i++;
                continue;
            }
            reservedBytes -= retained.block.capacity();
            retained.block.close();
            retainedMovedBlocks.remove(i);
        }
    }

    private long pinnedObjects() {
        long count = 0L;
        for (Long slotId : allocations.keySet()) {
            NativeHandle handle = handleForSlot(slotId);
            if (handle == null) {
                continue;
            }
            YierdisNativeObjectMeta meta = objectTable.snapshot(handle, true);
            if (meta.pinCount() > 0) {
                count++;
            }
        }
        return count;
    }

    private long quarantinedObjects() {
        long count = 0L;
        for (Allocation allocation : allocations.values()) {
            if (allocation.quarantined) {
                count++;
            }
        }
        return count;
    }

    private NativeHandle handleForSlot(long slotId) {
        Allocation allocation = allocations.get(slotId);
        if (allocation == null || allocation.lastHandle == null) {
            return null;
        }
        return allocation.lastHandle;
    }

    private static long packedAddress(YierdisNativeBlock block) {
        return ((long) block.pageId() << 32) | Integer.toUnsignedLong(block.pageOffset());
    }

    private static void copyPrefix(YierdisNativeBlock src, YierdisNativeBlock dst, int len) {
        byte[] scratch = new byte[Math.min(len, REALLOC_COPY_CHUNK_BYTES)];
        int offset = 0;
        while (offset < len) {
            int chunk = Math.min(scratch.length, len - offset);
            src.getBytes(offset, scratch, 0, chunk);
            dst.setBytes(offset, scratch, 0, chunk);
            offset += chunk;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("stable native allocator is closed");
        }
    }

    private static RuntimeException closeBlock(YierdisNativeBlock block, RuntimeException failure) {
        try {
            block.close();
            return failure;
        } catch (RuntimeException e) {
            return addFailure(failure, e);
        }
    }

    private static RuntimeException addFailure(RuntimeException failure, RuntimeException next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private final class AllocatorEpochScope implements NativeEpochScope {
        private final NativeEpochScope delegate;
        private boolean closedScope;

        private AllocatorEpochScope(NativeEpochScope delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public NativeEpochKind kind() {
            return delegate.kind();
        }

        @Override
        public long epoch() {
            return delegate.epoch();
        }

        @Override
        public void close() {
            synchronized (YierdisStableNativeAllocator.this) {
                if (closedScope) {
                    return;
                }
                closedScope = true;
                delegate.close();
                if (!closed) {
                    reclaimEligibleQuarantine();
                }
            }
        }
    }

    private final class StableObjectView implements NativeObjectView {
        private final NativeHandle handle;
        private final Allocation allocation;
        private final NativeAccessMode mode;
        private boolean closedView;

        private StableObjectView(NativeHandle handle, Allocation allocation, NativeAccessMode mode) {
            this.handle = handle;
            this.allocation = allocation;
            this.allocation.lastHandle = handle;
            this.mode = mode;
        }

        @Override
        public NativeHandle handle() {
            synchronized (YierdisStableNativeAllocator.this) {
                ensureLive();
                return handle;
            }
        }

        @Override
        public int size() {
            synchronized (YierdisStableNativeAllocator.this) {
                YierdisNativeObjectMeta meta = ensureLive();
                return meta.size();
            }
        }

        @Override
        public int capacity() {
            synchronized (YierdisStableNativeAllocator.this) {
                ensureLive();
                return allocation.current.capacity();
            }
        }

        @Override
        public byte getByte(int index) {
            synchronized (YierdisStableNativeAllocator.this) {
                YierdisNativeObjectMeta meta = ensureLive();
                checkRange(index, 1, meta.size());
                return allocation.current.getByte(index);
            }
        }

        @Override
        public void setByte(int index, byte value) {
            synchronized (YierdisStableNativeAllocator.this) {
                YierdisNativeObjectMeta meta = ensureWritable();
                checkRange(index, 1, meta.size());
                allocation.current.setByte(index, value);
            }
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            synchronized (YierdisStableNativeAllocator.this) {
                YierdisNativeObjectMeta meta = ensureLive();
                checkRange(index, len, meta.size());
                allocation.current.getBytes(index, dst, dstOff, len);
            }
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            synchronized (YierdisStableNativeAllocator.this) {
                YierdisNativeObjectMeta meta = ensureWritable();
                checkRange(index, len, meta.size());
                allocation.current.setBytes(index, src, srcOff, len);
            }
        }

        @Override
        public void close() {
            synchronized (YierdisStableNativeAllocator.this) {
                if (closedView) {
                    return;
                }
                closedView = true;
                if (!closed) {
                    unpin(handle);
                }
            }
        }

        private YierdisNativeObjectMeta ensureWritable() {
            YierdisNativeObjectMeta meta = ensureLive();
            if (mode != NativeAccessMode.READ_WRITE) {
                throw new NativeMemoryException("resolved object is read-only");
            }
            return meta;
        }

        private YierdisNativeObjectMeta ensureLive() {
            if (closedView) {
                throw new IllegalStateException("native object view is closed");
            }
            YierdisNativeObjectMeta meta = requireLiveMeta(handle);
            allocationFor(handle);
            return meta;
        }

        private void checkRange(int index, int len, int size) {
            if (len < 0 || index < 0 || index > size - len) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    private static final class Allocation {
        private YierdisNativeBlock current;
        private NativeHandle lastHandle;
        private boolean quarantined;
        private long retiredEpoch;

        private Allocation(YierdisNativeBlock current) {
            this.current = Objects.requireNonNull(current, "current");
        }
    }

    private record RetainedBlock(YierdisNativeBlock block, long retiredEpoch) {
        private RetainedBlock {
            Objects.requireNonNull(block, "block");
            if (retiredEpoch <= 0) {
                throw new IllegalArgumentException("retiredEpoch must be > 0");
            }
        }
    }

    @FunctionalInterface
    private interface MetaSupplier {
        YierdisNativeObjectMeta get();
    }
}
