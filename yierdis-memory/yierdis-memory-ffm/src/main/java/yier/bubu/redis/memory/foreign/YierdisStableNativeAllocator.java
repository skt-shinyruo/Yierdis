package yier.bubu.redis.memory.foreign;

import java.util.Arrays;
import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocationLatencyHistogram;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKindCounts;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public final class YierdisStableNativeAllocator implements NativeAllocator {
    private static final int REALLOC_COPY_CHUNK_BYTES = 64 * 1024;
    private static final long ALLOCATION_SCOPE_HEAP_BYTES = 96L;
    private static final long ARRAY_HEADER_BYTES = 16L;

    private final YierdisNativePageAllocator pageAllocator;
    private final YierdisNativeObjectTable objectTable;
    private final YierdisNativeEpochManager epochManager = new YierdisNativeEpochManager();
    private final YierdisNativeDefragValidator defragValidator;
    private final YierdisAllocatorThreadGuard threadGuard;
    private int[] retainedPageIds = new int[0];
    private int[] retainedPageOffsets = new int[0];
    private int[] retainedCapacities = new int[0];
    private int[] retainedPageClasses = new int[0];
    private long[] retainedEpochs = new long[0];
    private int retainedBlockCount;

    private boolean closed;
    private long logicalUsedBytes;
    private long reservedBytes;
    private long liveObjects;
    private long staleHandleDetections;
    private long reallocInPlaceCount;
    private long reallocMovedCount;
    private long defragMovedBytes;
    private long defragSkippedPinnedObjects;
    private long doubleFreeDetections;
    private long defragReclaimedPages;
    private long allocationCount;
    private long allocationTotalNanos;
    private long allocationMaxNanos;
    private long allocationUnder1Micros;
    private long allocationUnder10Micros;
    private long allocationUnder100Micros;
    private long allocationUnder1Millis;
    private long allocationAtLeast1Millis;
    private AllocatorAllocationScope activeAllocationScope;

    public YierdisStableNativeAllocator(YierdisFfmMemoryRuntime runtime, int maxSlots) {
        this(runtime, maxSlots, (handle, sourceMeta, target) -> {
        });
    }

    YierdisStableNativeAllocator(
            YierdisFfmMemoryRuntime runtime,
            int maxSlots,
            YierdisNativeDefragValidator defragValidator
    ) {
        this(runtime, maxSlots, defragValidator, new YierdisAllocatorThreadGuard(true));
    }

    YierdisStableNativeAllocator(
            YierdisFfmMemoryRuntime runtime,
            int maxSlots,
            YierdisNativeDefragValidator defragValidator,
            YierdisAllocatorThreadGuard threadGuard
    ) {
        Objects.requireNonNull(runtime, "runtime");
        this.defragValidator = Objects.requireNonNull(defragValidator, "defragValidator");
        this.threadGuard = Objects.requireNonNull(threadGuard, "threadGuard");
        this.pageAllocator = new YierdisNativePageAllocator(runtime);
        this.objectTable = new YierdisNativeObjectTable(runtime, maxSlots, 0);
    }

    @Override
    public void bindToCurrentThread() {
        threadGuard.bindToCurrentThread();
        ensureOpen();
    }

    @Override
    public NativeHandle allocate(NativeObjectKind kind, int size) {
        ensureOpen();
        Objects.requireNonNull(kind, "kind");

        long startedNanos = System.nanoTime();
        YierdisNativeBlock block = pageAllocator.allocate(physicalAllocationBytes(size));
        boolean allocated = false;
        try {
            NativeHandle handle = objectTable.allocate(
                    kind,
                    size,
                    block.capacity(),
                    block.pageId(),
                    block.pageOffset(),
                    block.pageClass().ordinal(),
                    epochManager.nextEpoch()
            );
            logicalUsedBytes += size;
            reservedBytes += block.capacity();
            liveObjects++;
            allocated = true;
            if (activeAllocationScope != null) {
                try {
                    activeAllocationScope.track(handle);
                } catch (RuntimeException | Error failure) {
                    free(handle);
                    throw failure;
                }
            }
            return handle;
        } finally {
            recordAllocationLatency(System.nanoTime() - startedNanos);
            if (!allocated) {
                block.close();
            }
        }
    }

    @Override
    public NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        ensureOpen();
        Objects.requireNonNull(policy, "policy");
        if (newSize < 0) {
            throw new IllegalArgumentException("newSize must be >= 0");
        }

        YierdisNativeObjectMeta meta = requireLiveMeta(handle);
        if (meta.pinCount() > 0) {
            throw new NativeMemoryException("native object is pinned");
        }

        int oldSize = meta.size();
        if (newSize <= meta.capacity()) {
            objectTable.updateLocation(
                    handle,
                    newSize,
                    meta.capacity(),
                    meta.segmentId(),
                    meta.address(),
                    meta.pageClass()
            );
            logicalUsedBytes += (long) newSize - oldSize;
            reallocInPlaceCount++;
            return handle;
        }

        if (policy == NativeReallocPolicy.NO_MOVE) {
            throw new NativeMemoryException("native object cannot grow in place");
        }

        YierdisNativeBlock previous = pageAllocator.moveSource(meta);
        YierdisNativeBlock next = pageAllocator.allocate(physicalAllocationBytes(newSize));
        boolean moved = false;
        try {
            copyPrefix(previous, next, oldSize);
            objectTable.updateLocation(
                    handle,
                    newSize,
                    next.capacity(),
                    next.pageId(),
                    next.pageOffset(),
                    next.pageClass().ordinal()
            );
            logicalUsedBytes += (long) newSize - oldSize;
            reserveMovedBlock(previous, next.capacity(), epochManager.nextEpoch());
            reallocMovedCount++;
            moved = true;
            if (activeAllocationScope != null) {
                activeAllocationScope.recordGrowth();
            }
            return handle;
        } finally {
            if (!moved) {
                next.close();
            }
        }
    }

    @Override
    public void free(NativeHandle handle) {
        ensureOpen();
        if (activeAllocationScope != null) {
            activeAllocationScope.recordGrowth();
        }
        YierdisNativeObjectMeta meta = requireLiveMetaForFree(handle);
        long freeEpoch = epochManager.nextEpoch();
        boolean delayRelease = meta.pinCount() > 0 || !epochManager.canReclaim(freeEpoch);
        objectTable.free(handle, freeEpoch, delayRelease);
        if (delayRelease) {
            reclaimEligibleQuarantine();
            untrackScopedHandle(handle);
            return;
        }
        releaseAllocation(meta);
        untrackScopedHandle(handle);
    }

    @Override
    public void pin(NativeHandle handle) {
        ensureOpen();
        trackStale(() -> {
            objectTable.pin(trackStale(handle));
            return objectTable.snapshot(handle, false);
        });
    }

    @Override
    public void unpin(NativeHandle handle) {
        ensureOpen();
        YierdisNativeObjectMeta before = objectMeta(handle, true);
        objectTable.unpin(trackStale(handle), false);
        if (before.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED && before.pinCount() == 1) {
            reclaimEligibleQuarantine();
        }
    }

    @Override
    public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        ensureOpen();
        NativeEpochScope delegate = epochManager.begin(kind);
        return new AllocatorEpochScope(delegate);
    }

    @Override
    public NativeAllocationScope beginAllocationScope() {
        ensureOpen();
        if (activeAllocationScope != null) {
            throw new IllegalStateException("native allocation scope is already active");
        }
        AllocatorAllocationScope scope = new AllocatorAllocationScope(
                memoryUsage(),
                objectTable.allocationScopeCheckpoint(),
                pageAllocator.beginAllocationScope()
        );
        activeAllocationScope = scope;
        return scope;
    }

    @Override
    public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        ensureOpen();
        Objects.requireNonNull(mode, "mode");
        YierdisNativeObjectMeta meta = requireLiveMeta(handle);
        objectTable.pin(trackStale(handle));
        try {
            return new StableObjectView(handle, pageAllocator.view(meta), mode);
        } catch (RuntimeException e) {
            objectTable.unpin(handle);
            throw e;
        }
    }

    @Override
    public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
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

        return moveLiveObject(handle);
    }

    @Override
    public NativeDefragReport defragCycle(NativeDefragOptions options) {
        ensureOpen();
        Objects.requireNonNull(options, "options");

        YierdisNativeDefragPlanner planner = new YierdisNativeDefragPlanner(options);
        long movedObjects = 0L;
        long skippedPinnedObjects = 0L;
        long skippedBudgetObjects = 0L;
        long failedMoves = 0L;

        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta == null) {
                continue;
            }
            if (meta.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                continue;
            }
            if (meta.state() != YierdisNativeObjectTable.STATE_ALLOCATED
                    && meta.state() != YierdisNativeObjectTable.STATE_PINNED) {
                continue;
            }
            if (!planner.canInspectNext()) {
                break;
            }
            planner.onCandidateInspected();

            if (meta.pinCount() > 0) {
                skippedPinnedObjects++;
                defragSkippedPinnedObjects++;
                continue;
            }
            if (!planner.canMove(meta.size())) {
                skippedBudgetObjects++;
                break;
            }

            try {
                NativeHandle handle = handleFor(meta);
                NativeDefragResult result = moveLiveObject(handle);
                if (result.moved()) {
                    movedObjects++;
                    planner.onMoved(result.movedBytes());
                }
            } catch (RuntimeException e) {
                failedMoves++;
            }
        }

        reclaimEligibleQuarantine();
        return new NativeDefragReport(
                planner.scannedObjects(),
                movedObjects,
                planner.movedBytes(),
                skippedPinnedObjects,
                skippedBudgetObjects,
                failedMoves,
                planner.stoppedByByteBudget(),
                planner.stoppedByObjectBudget(),
                planner.stoppedByTimeBudget()
        );
    }

    @Override
    public long logicalUsedBytes() {
        ensureOpen();
        return Math.max(0L, logicalUsedBytes);
    }

    @Override
    public NativeAllocatorStats stats() {
        ensureOpen();
        YierdisNativePageAllocatorStats pageStats = pageAllocator.stats();
        YierdisNativeObjectTableStats tableStats = objectTable.stats();
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
                defragSkippedPinnedObjects,
                Math.max(0L, pageStats.freeBytes() - retainedMovedBlockBytes()),
                pageStats.smallFreeBytes(),
                pageStats.mediumFreeBytes(),
                pageStats.largeFreeBytes(),
                pageStats.freePages(),
                quarantineBytes(),
                doubleFreeDetections,
                defragReclaimedPages,
                objectKindCounts(),
                allocationLatencyHistogram(),
                tableStats.metadataCommittedBytes(),
                tableStats.activeSegments(),
                tableStats.freeSlots(),
                tableStats.retiredSlots(),
                tableStats.peakLiveSlots()
        );
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        ensureOpen();
        YierdisNativePageAllocatorStats pageStats = pageAllocator.stats();
        YierdisNativeObjectTableStats tableStats = objectTable.stats();
        long heapBytes = MemoryUsageSnapshot.addSaturating(
                objectTable.heapEstimatedBytes(),
                pageAllocator.heapEstimatedBytes()
        );
        heapBytes = MemoryUsageSnapshot.addSaturating(heapBytes, retainedLocationHeapBytes());
        if (activeAllocationScope != null) {
            heapBytes = MemoryUsageSnapshot.addSaturating(
                    heapBytes,
                    activeAllocationScope.heapEstimatedBytes()
            );
        }
        return new MemoryUsageSnapshot(
                heapBytes,
                tableStats.metadataCommittedBytes(),
                pageStats.committedBytes(),
                pageStats.usedBytes(),
                (long) pageStats.emptySmallPages() * YierdisNativePageAllocator.PAGE_BYTES
        );
    }

    @Override
    public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        ensureOpen();
        MemoryReclaimResult result = pageAllocator.trimEmptyPages(budget);
        defragReclaimedPages = MemoryUsageSnapshot.addSaturating(
                defragReclaimedPages,
                result.reclaimedUnits()
        );
        return result;
    }

    @Override
    public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        ensureOpen();
        Objects.requireNonNull(requestedBytes, "requestedBytes");
        for (int requested : requestedBytes) {
            if (requested <= 0) {
                throw new IllegalArgumentException("requestedBytes must contain only positive values");
            }
        }
        int additionalSegments = objectTable.estimateAdditionalSegments(requestedBytes.length);
        long metadataBytes = (long) additionalSegments
                * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT
                * YierdisNativeObjectTable.META_BYTES;
        YierdisNativePageAllocator.PageGrowth pageGrowth = pageAllocator.estimateAdditionalGrowth(requestedBytes);
        long heapBytes = MemoryUsageSnapshot.addSaturating(
                objectTable.estimateAdditionalHeapBytes(requestedBytes.length),
                pageGrowth.heapEstimatedBytes()
        );
        return new NativeAllocationGrowth(heapBytes, metadataBytes, pageGrowth.nativeDataCommittedBytes());
    }

    @Override
    public void close() {
        threadGuard.checkOrBindCurrentThread();
        if (closed) {
            return;
        }
        if (activeAllocationScope != null) {
            activeAllocationScope.abort();
        }
        closed = true;

        long leakedObjects = liveObjects;
        RuntimeException failure = leakedObjects == 0L
                ? null
                : new IllegalStateException("native allocator closed with " + leakedObjects + " live objects");
        retainedPageIds = new int[0];
        retainedPageOffsets = new int[0];
        retainedCapacities = new int[0];
        retainedPageClasses = new int[0];
        retainedEpochs = new long[0];
        retainedBlockCount = 0;
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

    YierdisNativeObjectMeta objectMeta(NativeHandle handle, boolean allowQuarantined) {
        ensureOpen();
        return trackStale(() -> objectTable.snapshot(handle, allowQuarantined));
    }

    private YierdisNativeObjectMeta requireLiveMeta(NativeHandle handle) {
        return trackStale(() -> objectTable.resolve(handle));
    }

    private YierdisNativeObjectMeta requireLiveMetaForFree(NativeHandle handle) {
        try {
            return requireLiveMeta(handle);
        } catch (StaleNativeHandleException e) {
            doubleFreeDetections++;
            throw e;
        }
    }

    private static int physicalAllocationBytes(int logicalSize) {
        if (logicalSize < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
        return Math.max(1, logicalSize);
    }

    private NativeDefragResult moveLiveObject(NativeHandle handle) {
        YierdisNativeObjectMeta sourceMeta = objectTable.beginMove(handle);
        YierdisNativeBlock previous = null;
        YierdisNativeBlock target = null;
        boolean published = false;
        try {
            previous = pageAllocator.moveSource(sourceMeta);
            target = pageAllocator.allocate(physicalAllocationBytes(sourceMeta.size()));
            copyPrefix(previous, target, sourceMeta.size());
            defragValidator.validate(handle, sourceMeta, target);

            int targetCapacity = target.capacity();
            objectTable.publishMoved(
                    handle,
                    sourceMeta.size(),
                    targetCapacity,
                    target.pageId(),
                    target.pageOffset(),
                    target.pageClass().ordinal()
            );
            published = true;
            target = null;
            long retiredBytes = previous.capacity();
            reserveMovedBlock(previous, targetCapacity, epochManager.nextEpoch());
            defragReclaimedPages += retiredBytes / YierdisNativePageAllocator.PAGE_BYTES;
            defragMovedBytes += sourceMeta.size();
            return NativeDefragResult.moved(sourceMeta.size());
        } catch (RuntimeException e) {
            if (!published) {
                try {
                    objectTable.abortMove(handle);
                } catch (RuntimeException abortFailure) {
                    e.addSuppressed(abortFailure);
                }
            }
            throw e;
        } finally {
            if (target != null) {
                target.close();
            }
        }
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

    private void releaseAllocation(YierdisNativeObjectMeta meta) {
        logicalUsedBytes -= meta.size();
        reservedBytes -= meta.capacity();
        liveObjects--;
        pageAllocator.free(meta);
    }

    private void untrackScopedHandle(NativeHandle handle) {
        if (activeAllocationScope != null) {
            activeAllocationScope.untrack(handle);
        }
    }

    private void releaseQuarantinedAllocation(YierdisNativeObjectMeta meta) {
        objectTable.releaseQuarantined(handleFor(meta));
        releaseAllocation(meta);
    }

    private void reserveMovedBlock(YierdisNativeBlock block, int nextCapacity, long retiredEpoch) {
        if (epochManager.canReclaim(retiredEpoch)) {
            block.close();
            reservedBytes += (long) nextCapacity - block.capacity();
            return;
        }
        reservedBytes += nextCapacity;
        ensureRetainedCapacity(retainedBlockCount + 1);
        retainedPageIds[retainedBlockCount] = block.pageId();
        retainedPageOffsets[retainedBlockCount] = block.pageOffset();
        retainedCapacities[retainedBlockCount] = block.capacity();
        retainedPageClasses[retainedBlockCount] = block.pageClass().ordinal();
        retainedEpochs[retainedBlockCount] = retiredEpoch;
        retainedBlockCount++;
    }

    private void reclaimEligibleQuarantine() {
        reclaimEligibleFreedObjects();
        reclaimEligibleMovedBlocks();
    }

    private void reclaimEligibleFreedObjects() {
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta == null || meta.state() != YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                continue;
            }
            if (!epochManager.canReclaim(meta.freeEpoch())) {
                continue;
            }
            if (meta.pinCount() != 0) {
                continue;
            }
            releaseQuarantinedAllocation(meta);
        }
    }

    private void reclaimEligibleMovedBlocks() {
        for (int i = 0; i < retainedBlockCount; ) {
            if (!epochManager.canReclaim(retainedEpochs[i])) {
                i++;
                continue;
            }
            reservedBytes -= retainedCapacities[i];
            pageAllocator.free(
                    retainedPageIds[i],
                    retainedPageOffsets[i],
                    retainedCapacities[i],
                    retainedPageClasses[i]
            );
            removeRetainedLocation(i);
        }
    }

    private long pinnedObjects() {
        long count = 0L;
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta.pinCount() > 0) {
                count++;
            }
        }
        return count;
    }

    private long quarantinedObjects() {
        long count = 0L;
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                count++;
            }
        }
        return count;
    }

    private long quarantineBytes() {
        long bytes = retainedMovedBlockBytes();
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                bytes += meta.capacity();
            }
        }
        return bytes;
    }

    private long retainedMovedBlockBytes() {
        long bytes = 0L;
        for (int i = 0; i < retainedBlockCount; i++) {
            bytes += retainedCapacities[i];
        }
        return bytes;
    }

    private long retainedLocationHeapBytes() {
        return 80L
                + (long) retainedPageIds.length * Integer.BYTES
                + (long) retainedPageOffsets.length * Integer.BYTES
                + (long) retainedCapacities.length * Integer.BYTES
                + (long) retainedPageClasses.length * Integer.BYTES
                + (long) retainedEpochs.length * Long.BYTES;
    }

    private NativeObjectKindCounts objectKindCounts() {
        long generic = 0;
        long stringBytes = 0;
        long listpackBytes = 0;
        long hashFieldBytes = 0;
        long hashValueBytes = 0;
        long setMemberBytes = 0;
        long zsetMemberBytes = 0;
        long scoreBytes = 0;
        long entryRecords = 0;
        long keyBytes = 0;
        long listRoots = 0;
        long hashRoots = 0;
        long setRoots = 0;
        long zsetRoots = 0;
        long listNodes = 0;
        long hashTables = 0;
        long setTables = 0;
        long zsetTables = 0;
        long zsetNodes = 0;
        long indexNodes = 0;
        long metadataRecords = 0;
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta == null || meta.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                continue;
            }
            NativeObjectKind kind = kindFor(meta);
            if (kind == null) {
                continue;
            }
            switch (kind) {
                case GENERIC -> generic++;
                case STRING_BYTES -> stringBytes++;
                case LISTPACK_BYTES -> listpackBytes++;
                case HASH_FIELD_BYTES -> hashFieldBytes++;
                case HASH_VALUE_BYTES -> hashValueBytes++;
                case SET_MEMBER_BYTES -> setMemberBytes++;
                case ZSET_MEMBER_BYTES -> zsetMemberBytes++;
                case SCORE_BYTES -> scoreBytes++;
                case ENTRY_RECORD -> entryRecords++;
                case KEY_BYTES -> keyBytes++;
                case LIST_ROOT -> listRoots++;
                case HASH_ROOT -> hashRoots++;
                case SET_ROOT -> setRoots++;
                case ZSET_ROOT -> zsetRoots++;
                case LIST_NODE -> listNodes++;
                case HASH_TABLE -> hashTables++;
                case SET_TABLE -> setTables++;
                case ZSET_TABLE -> zsetTables++;
                case ZSET_NODE -> zsetNodes++;
                case INDEX_NODE -> indexNodes++;
                case METADATA_RECORD -> metadataRecords++;
            }
        }
        return new NativeObjectKindCounts(
                generic,
                stringBytes,
                listpackBytes,
                hashFieldBytes,
                hashValueBytes,
                setMemberBytes,
                zsetMemberBytes,
                scoreBytes,
                entryRecords,
                keyBytes,
                listRoots,
                hashRoots,
                setRoots,
                zsetRoots,
                listNodes,
                hashTables,
                setTables,
                zsetTables,
                zsetNodes,
                indexNodes,
                metadataRecords
        );
    }

    private NativeAllocationLatencyHistogram allocationLatencyHistogram() {
        return new NativeAllocationLatencyHistogram(
                allocationCount,
                allocationTotalNanos,
                allocationMaxNanos,
                allocationUnder1Micros,
                allocationUnder10Micros,
                allocationUnder100Micros,
                allocationUnder1Millis,
                allocationAtLeast1Millis
        );
    }

    private void recordAllocationLatency(long nanos) {
        long safeNanos = Math.max(0L, nanos);
        allocationCount++;
        allocationTotalNanos += safeNanos;
        allocationMaxNanos = Math.max(allocationMaxNanos, safeNanos);
        if (safeNanos < 1_000L) {
            allocationUnder1Micros++;
        } else if (safeNanos < 10_000L) {
            allocationUnder10Micros++;
        } else if (safeNanos < 100_000L) {
            allocationUnder100Micros++;
        } else if (safeNanos < 1_000_000L) {
            allocationUnder1Millis++;
        } else {
            allocationAtLeast1Millis++;
        }
    }

    private static NativeObjectKind kindFor(YierdisNativeObjectMeta meta) {
        for (NativeObjectKind kind : NativeObjectKind.values()) {
            if (kind.domain() == meta.domain() && kind.code() == meta.kindCode()) {
                return kind;
            }
        }
        return null;
    }

    private static NativeHandle handleFor(YierdisNativeObjectMeta meta) {
        NativeObjectKind kind = kindFor(meta);
        if (kind == null) {
            throw new NativeMemoryException("unknown native object kind: " + meta.kindCode());
        }
        return NativeHandle.of(meta.domain(), kind, meta.slotId(), meta.generation(), meta.flags() & 0x0f);
    }

    private void ensureRetainedCapacity(int required) {
        if (required <= retainedPageIds.length) {
            return;
        }
        int capacity = retainedPageIds.length == 0
                ? Math.max(4, required)
                : Math.max(required, retainedPageIds.length + Math.max(1, retainedPageIds.length >>> 1));
        retainedPageIds = Arrays.copyOf(retainedPageIds, capacity);
        retainedPageOffsets = Arrays.copyOf(retainedPageOffsets, capacity);
        retainedCapacities = Arrays.copyOf(retainedCapacities, capacity);
        retainedPageClasses = Arrays.copyOf(retainedPageClasses, capacity);
        retainedEpochs = Arrays.copyOf(retainedEpochs, capacity);
    }

    private void removeRetainedLocation(int index) {
        int last = --retainedBlockCount;
        if (index != last) {
            retainedPageIds[index] = retainedPageIds[last];
            retainedPageOffsets[index] = retainedPageOffsets[last];
            retainedCapacities[index] = retainedCapacities[last];
            retainedPageClasses[index] = retainedPageClasses[last];
            retainedEpochs[index] = retainedEpochs[last];
        }
        retainedPageIds[last] = 0;
        retainedPageOffsets[last] = 0;
        retainedCapacities[last] = 0;
        retainedPageClasses[last] = 0;
        retainedEpochs[last] = 0L;
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
        threadGuard.checkOrBindCurrentThread();
        if (closed) {
            throw new IllegalStateException("stable native allocator is closed");
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
            threadGuard.checkOrBindCurrentThread();
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

    private final class AllocatorAllocationScope implements NativeAllocationScope {
        private final MemoryUsageSnapshot baseline;
        private final YierdisNativeObjectTable.AllocationScopeCheckpoint tableCheckpoint;
        private final YierdisNativePageAllocator.AllocationScopeCheckpoint pageCheckpoint;
        private long[] handles = new long[0];
        private int handleCount;
        private NativeAllocationGrowth peakGrowth = NativeAllocationGrowth.zero();
        private boolean terminal;

        private AllocatorAllocationScope(
                MemoryUsageSnapshot baseline,
                YierdisNativeObjectTable.AllocationScopeCheckpoint tableCheckpoint,
                YierdisNativePageAllocator.AllocationScopeCheckpoint pageCheckpoint
        ) {
            this.baseline = baseline;
            this.tableCheckpoint = tableCheckpoint;
            this.pageCheckpoint = pageCheckpoint;
        }

        @Override
        public NativeAllocationGrowth growth() {
            if (terminal) {
                return NativeAllocationGrowth.zero();
            }
            recordGrowth();
            return peakGrowth;
        }

        private void recordGrowth() {
            MemoryUsageSnapshot current = memoryUsage();
            NativeAllocationGrowth currentGrowth = new NativeAllocationGrowth(
                    positiveDifference(current.heapEstimatedBytes(), baseline.heapEstimatedBytes()),
                    positiveDifference(current.nativeMetadataCommittedBytes(), baseline.nativeMetadataCommittedBytes()),
                    positiveDifference(
                            current.nativeDataCommittedBytes(),
                            baseline.nativeDataCommittedBytes()
                    )
            );
            peakGrowth = new NativeAllocationGrowth(
                    Math.max(peakGrowth.heapEstimatedBytes(), currentGrowth.heapEstimatedBytes()),
                    Math.max(
                            peakGrowth.nativeMetadataCommittedBytes(),
                            currentGrowth.nativeMetadataCommittedBytes()
                    ),
                    Math.max(
                            peakGrowth.nativeDataCommittedBytes(),
                            currentGrowth.nativeDataCommittedBytes()
                    )
            );
        }

        @Override
        public void promote() {
            if (terminal) {
                return;
            }
            pageAllocator.promoteAllocationScope(pageCheckpoint);
            terminal = true;
            handleCount = 0;
            activeAllocationScope = null;
        }

        @Override
        public void abort() {
            if (terminal) {
                return;
            }
            terminal = true;
            activeAllocationScope = null;
            RuntimeException failure = null;
            for (int i = handleCount - 1; i >= 0; i--) {
                try {
                    free(NativeHandle.fromRaw(handles[i]));
                } catch (RuntimeException releaseFailure) {
                    failure = addFailure(failure, releaseFailure);
                }
            }
            handleCount = 0;
            try {
                pageAllocator.restoreAllocationScope(pageCheckpoint);
            } catch (RuntimeException trimFailure) {
                failure = addFailure(failure, trimFailure);
            }
            try {
                objectTable.restoreAllocationScopeCheckpoint(tableCheckpoint);
            } catch (RuntimeException trimFailure) {
                failure = addFailure(failure, trimFailure);
            }
            if (failure != null) {
                throw failure;
            }
        }

        private void track(NativeHandle handle) {
            if (terminal) {
                throw new IllegalStateException("native allocation scope is closed");
            }
            if (handleCount == handles.length) {
                handles = Arrays.copyOf(handles, Math.max(8, handles.length << 1));
            }
            handles[handleCount++] = handle.raw();
            recordGrowth();
        }

        private long heapEstimatedBytes() {
            return ALLOCATION_SCOPE_HEAP_BYTES
                    + arrayHeapBytes(handles.length, Long.BYTES)
                    + tableCheckpoint.heapEstimatedBytes()
                    + pageCheckpoint.heapEstimatedBytes();
        }

        private void untrack(NativeHandle handle) {
            long raw = handle.raw();
            for (int i = handleCount - 1; i >= 0; i--) {
                if (handles[i] != raw) {
                    continue;
                }
                handles[i] = handles[--handleCount];
                return;
            }
        }

        private long positiveDifference(long current, long before) {
            return current > before ? current - before : 0L;
        }
    }

    private static long arrayHeapBytes(int length, long elementBytes) {
        return ARRAY_HEADER_BYTES + (long) length * elementBytes;
    }

    private final class StableObjectView implements NativeObjectView {
        private final NativeHandle handle;
        private final YierdisNativeBlock block;
        private final NativeAccessMode mode;
        private boolean closedView;

        private StableObjectView(NativeHandle handle, YierdisNativeBlock block, NativeAccessMode mode) {
            this.handle = handle;
            this.block = block;
            this.mode = mode;
        }

        @Override
        public NativeHandle handle() {
            ensureLive();
            return handle;
        }

        @Override
        public int size() {
            YierdisNativeObjectMeta meta = ensureLive();
            return meta.size();
        }

        @Override
        public int capacity() {
            ensureLive();
            return block.capacity();
        }

        @Override
        public byte getByte(int index) {
            YierdisNativeObjectMeta meta = ensureLive();
            checkRange(index, 1, meta.size());
            return block.getByte(index);
        }

        @Override
        public void setByte(int index, byte value) {
            YierdisNativeObjectMeta meta = ensureWritable();
            checkRange(index, 1, meta.size());
            block.setByte(index, value);
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            YierdisNativeObjectMeta meta = ensureLive();
            checkRange(index, len, meta.size());
            block.getBytes(index, dst, dstOff, len);
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            YierdisNativeObjectMeta meta = ensureWritable();
            checkRange(index, len, meta.size());
            block.setBytes(index, src, srcOff, len);
        }

        @Override
        public void close() {
            threadGuard.checkOrBindCurrentThread();
            if (closedView) {
                return;
            }
            closedView = true;
            if (!closed) {
                unpin(handle);
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
            ensureOpen();
            if (closedView) {
                throw new IllegalStateException("native object view is closed");
            }
            YierdisNativeObjectMeta meta = requireLiveMeta(handle);
            if (meta.segmentId() != block.pageId()
                    || meta.address() != block.pageOffset()
                    || meta.capacity() != block.capacity()
                    || meta.pageClass() != block.pageClass().ordinal()) {
                throw new NativeMemoryException("resolved native object location changed while pinned");
            }
            return meta;
        }

        private void checkRange(int index, int len, int size) {
            if (len < 0 || index < 0 || index > size - len) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    @FunctionalInterface
    private interface MetaSupplier {
        YierdisNativeObjectMeta get();
    }
}
