package yier.bubu.redis.memory.foreign;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

public final class YierdisNativePageAllocator
        implements AutoCloseable, YierdisNativeObjectTable.CapacityResolver {
    public static final int PAGE_BYTES = 64 * 1024;
    private static final int MEDIUM_MAX_BYTES = 1024 * 1024;
    private static final long SMALL_PAGE_HEAP_BYTES = 160L;
    private static final long SPAN_HEAP_BYTES = 112L;
    private static final long REGION_WRAPPER_HEAP_BYTES = 128L;
    private static final long ALLOCATION_SCOPE_CHECKPOINT_HEAP_BYTES = 40L;
    private static final YierdisNativePageClass[] PAGE_CLASSES = YierdisNativePageClass.values();

    private final YierdisFfmMemoryRuntime runtime;
    private final YierdisNativePageDirectory pageDirectory = new YierdisNativePageDirectory();
    private final SmallPage[] nonFullHeads = new SmallPage[YierdisNativeSizeClass.count()];
    private final SmallPage[] emptyByClass = new SmallPage[YierdisNativeSizeClass.count()];
    private final long[] freeBlocksByClass = new long[YierdisNativeSizeClass.count()];

    private SmallPage liveSmallHead;
    private SmallPage emptyHead;
    private SpanAllocation liveSpanHead;
    private boolean closed;
    private long committedBytes;
    private long usedBytes;
    private long smallFreeBytes;
    private long liveSmallPages;
    private long liveMediumSpanPages;
    private long liveLargeSpanPages;
    private long emptySmallPages;
    private long liveSpanDescriptors;
    private long nextPageSequence;
    private long retainedPageHeapBytes;
    private AllocationScopeCheckpoint activeAllocationScope;
    private boolean allocationScopeAbortInProgress;
    private boolean heapIterationTrapForTesting;

    public YierdisNativePageAllocator(YierdisFfmMemoryRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    public YierdisNativeBlock allocate(int requestedBytes) {
        ensureOpen();
        if (requestedBytes <= 0) {
            throw new IllegalArgumentException("requestedBytes must be > 0");
        }
        if (requestedBytes <= YierdisNativeSizeClass.MAX_SMALL_BYTES) {
            return allocateSmall(requestedBytes);
        }
        return allocateSpan(requestedBytes);
    }

    public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        ensureOpen();
        Objects.requireNonNull(budget, "budget");
        long startedNanos = System.nanoTime();
        long inspected = 0L;
        long reclaimedUnits = 0L;
        long reclaimedBytes = 0L;
        while (emptyHead != null) {
            if (inspected >= budget.maxInspectedUnits()) {
                return new MemoryReclaimResult(
                        inspected,
                        reclaimedUnits,
                        reclaimedBytes,
                        MemoryReclaimResult.StopReason.INSPECTION_LIMIT
                );
            }
            if (elapsedNanos(startedNanos) >= budget.timeLimitNanos()) {
                return new MemoryReclaimResult(
                        inspected,
                        reclaimedUnits,
                        reclaimedBytes,
                        MemoryReclaimResult.StopReason.TIME_LIMIT
                );
            }
            SmallPage page = emptyHead;
            inspected++;
            if (reclaimedBytes > budget.maxReclaimedBytes() - PAGE_BYTES) {
                return new MemoryReclaimResult(
                        inspected,
                        reclaimedUnits,
                        reclaimedBytes,
                        MemoryReclaimResult.StopReason.BYTE_LIMIT
                );
            }
            closeSmallPage(page);
            reclaimedUnits++;
            reclaimedBytes += PAGE_BYTES;
        }
        return new MemoryReclaimResult(
                inspected,
                reclaimedUnits,
                reclaimedBytes,
                MemoryReclaimResult.StopReason.COMPLETE
        );
    }

    public YierdisNativePageAllocatorStats stats() {
        return new YierdisNativePageAllocatorStats(
                committedBytes,
                usedBytes,
                committedBytes - usedBytes,
                liveSmallPages,
                liveMediumSpanPages,
                liveLargeSpanPages,
                smallFreeBytes,
                0,
                0,
                emptySmallPages,
                emptySmallPages,
                pageDirectory.liveEntries(),
                liveSpanDescriptors,
                pageDirectory.heapEstimatedBytes()
        );
    }

    PageGrowth estimateAdditionalGrowth(int... requestedBytes) {
        return estimateAdditionalGrowth(true, false, requestedBytes);
    }

    PageGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        return estimateAdditionalGrowth(true, true, requestedBytes);
    }

    private PageGrowth estimateAdditionalGrowth(
            boolean useAvailableSmallBlocks,
            boolean includeDirectoryRematerializationMargin,
            int... requestedBytes
    ) {
        Objects.requireNonNull(requestedBytes, "requestedBytes");
        long[] availableBlocks = useAvailableSmallBlocks
                ? freeBlocksByClass.clone()
                : new long[YierdisNativeSizeClass.count()];
        int additionalEntries = 0;
        long heapBytes = 0L;
        long dataBytes = 0L;
        for (int requested : requestedBytes) {
            if (requested <= 0) {
                throw new IllegalArgumentException("requestedBytes must contain only positive values");
            }
            if (requested <= YierdisNativeSizeClass.MAX_SMALL_BYTES) {
                YierdisNativeSizeClass sizeClass = YierdisNativeSizeClass.forSize(requested);
                int index = sizeClass.ordinal();
                if (availableBlocks[index] > 0) {
                    availableBlocks[index]--;
                    continue;
                }
                int blockCount = PAGE_BYTES / sizeClass.bytes();
                availableBlocks[index] = blockCount - 1L;
                additionalEntries++;
                dataBytes = MemoryUsageSnapshot.addSaturating(dataBytes, PAGE_BYTES);
                heapBytes = MemoryUsageSnapshot.addSaturating(
                        heapBytes,
                        SMALL_PAGE_HEAP_BYTES + REGION_WRAPPER_HEAP_BYTES + (long) blockCount * Integer.BYTES
                );
            } else {
                int capacity = Math.multiplyExact(pagesFor(requested), PAGE_BYTES);
                additionalEntries++;
                dataBytes = MemoryUsageSnapshot.addSaturating(dataBytes, capacity);
                heapBytes = MemoryUsageSnapshot.addSaturating(
                        heapBytes,
                        SPAN_HEAP_BYTES + REGION_WRAPPER_HEAP_BYTES
                );
            }
        }
        heapBytes = MemoryUsageSnapshot.addSaturating(
                heapBytes,
                pageDirectory.estimateAdditionalHeapBytes(additionalEntries)
        );
        if (includeDirectoryRematerializationMargin) {
            heapBytes = MemoryUsageSnapshot.addSaturating(
                    heapBytes,
                    pageDirectory.worstCaseSegmentRematerializationHeapBytes(additionalEntries)
            );
        }
        return new PageGrowth(heapBytes, dataBytes);
    }

    long heapEstimatedBytes() {
        long bytes = pageDirectory.heapEstimatedBytes();
        bytes = MemoryUsageSnapshot.addSaturating(bytes, (long) nonFullHeads.length * 8L);
        bytes = MemoryUsageSnapshot.addSaturating(bytes, (long) emptyByClass.length * 8L);
        bytes = MemoryUsageSnapshot.addSaturating(bytes, (long) freeBlocksByClass.length * Long.BYTES);
        return MemoryUsageSnapshot.addSaturating(bytes, retainedPageHeapBytes);
    }

    void armHeapIterationTrapsForTesting() {
        heapIterationTrapForTesting = true;
        pageDirectory.armHeapIterationTrapForTesting();
    }

    void disarmHeapIterationTrapsForTesting() {
        heapIterationTrapForTesting = false;
        pageDirectory.disarmHeapIterationTrapForTesting();
    }

    void armAllocationScopeAbortAllocationTrackingForTesting() {
        pageDirectory.armAllocationScopeAbortAllocationTrackingForTesting();
    }

    void disarmAllocationScopeAbortAllocationTrackingForTesting() {
        pageDirectory.disarmAllocationScopeAbortAllocationTrackingForTesting();
    }

    boolean allocationScopeAbortAllocatedForTesting() {
        return pageDirectory.allocationScopeAbortAllocatedForTesting();
    }

    AllocationScopeCheckpoint beginAllocationScope() {
        ensureOpen();
        if (activeAllocationScope != null) {
            throw new IllegalStateException("native page allocation scope is already active");
        }
        AllocationScopeCheckpoint checkpoint = new AllocationScopeCheckpoint(
                liveSmallHead,
                liveSpanHead,
                nextPageSequence,
                pageDirectory.allocationScopeCheckpoint()
        );
        activeAllocationScope = checkpoint;
        return checkpoint;
    }

    long allocationScopeCheckpointHeapEstimatedBytes() {
        ensureOpen();
        return ALLOCATION_SCOPE_CHECKPOINT_HEAP_BYTES
                + pageDirectory.allocationScopeCheckpointHeapEstimatedBytes();
    }

    void promoteAllocationScope(AllocationScopeCheckpoint checkpoint) {
        if (activeAllocationScope == checkpoint) {
            pageDirectory.promoteAllocationScope(checkpoint.directoryCheckpoint);
            activeAllocationScope = null;
            checkpoint.releaseReferences();
        }
    }

    void beginAllocationScopeAbort(AllocationScopeCheckpoint checkpoint) {
        ensureOpen();
        if (activeAllocationScope != checkpoint) {
            throw new IllegalStateException("native page allocation scope is not active");
        }
        pageDirectory.beginAllocationScopeAbort(checkpoint.directoryCheckpoint);
        allocationScopeAbortInProgress = true;
    }

    void restoreAllocationScope(AllocationScopeCheckpoint checkpoint) {
        ensureOpen();
        if (activeAllocationScope != checkpoint) {
            throw new IllegalStateException("native page allocation scope is not active");
        }
        try {
            while (liveSmallHead != checkpoint.smallPageHead) {
                if (liveSmallHead == null || liveSmallHead.liveBlocks != 0) {
                    throw new IllegalStateException("allocation scope left a live native small page");
                }
                closeSmallPage(liveSmallHead);
            }
            if (liveSpanHead != checkpoint.spanHead) {
                throw new IllegalStateException("allocation scope left a live native span");
            }
            pageDirectory.restoreAllocationScopeCheckpoint(checkpoint.directoryCheckpoint);
            nextPageSequence = checkpoint.nextPageSequence;
        } finally {
            allocationScopeAbortInProgress = false;
            pageDirectory.discardAllocationScope(checkpoint.directoryCheckpoint);
            activeAllocationScope = null;
            checkpoint.releaseReferences();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        while (liveSmallHead != null) {
            failure = closeSmallPageOnShutdown(liveSmallHead, failure);
        }
        while (liveSpanHead != null) {
            failure = closeSpanOnShutdown(liveSpanHead, failure);
        }
        pageDirectory.clear();
        committedBytes = 0L;
        usedBytes = 0L;
        smallFreeBytes = 0L;
        liveSmallPages = 0L;
        liveMediumSpanPages = 0L;
        liveLargeSpanPages = 0L;
        emptySmallPages = 0L;
        liveSpanDescriptors = 0L;
        retainedPageHeapBytes = 0L;
        if (failure != null) {
            throw failure;
        }
    }

    void free(YierdisNativeBlock block) {
        if (closed) {
            return;
        }
        Object allocation = block.allocation();
        if (allocation instanceof SmallPage page) {
            freeSmall(block, page);
            return;
        }
        if (allocation instanceof SpanAllocation span) {
            freeSpan(block, span);
            return;
        }
        throw new IllegalStateException("unknown native block allocation");
    }

    @Override
    public int resolveCapacity(int pageId, int pageOffset, int pageClassOrdinal) {
        ensureOpen();
        if (pageId <= 0 || pageOffset < 0) {
            throw new IllegalStateException("invalid native block location");
        }
        if (pageClassOrdinal < 0 || pageClassOrdinal >= PAGE_CLASSES.length) {
            throw new IllegalStateException("invalid native page class: " + pageClassOrdinal);
        }
        YierdisNativePageClass pageClass = PAGE_CLASSES[pageClassOrdinal];
        Object entry = pageDirectory.get(pageId);
        if (entry instanceof SmallPage page) {
            int capacity = page.sizeClass.bytes();
            if (page.closed
                    || pageClass != YierdisNativePageClass.SMALL
                    || pageOffset % capacity != 0
                    || pageOffset > PAGE_BYTES - capacity) {
                throw new IllegalStateException("native small block location mismatch");
            }
            return capacity;
        }
        if (entry instanceof SpanAllocation span) {
            if (span.closed || pageClass != span.pageClass || pageOffset != 0) {
                throw new IllegalStateException("native span location mismatch");
            }
            return span.capacity;
        }
        throw new IllegalStateException("unknown or closed native page id: " + pageId);
    }

    YierdisNativeBlock view(YierdisNativeObjectMeta meta) {
        ensureOpen();
        Objects.requireNonNull(meta, "meta");
        return blockAt(
                meta.segmentId(),
                Math.toIntExact(meta.address()),
                meta.capacity(),
                meta.pageClass(),
                Math.max(1, meta.size())
        );
    }

    YierdisNativeBlock moveSource(YierdisNativeObjectMeta meta) {
        return view(meta);
    }

    void free(YierdisNativeObjectMeta meta) {
        YierdisNativeBlock block = view(meta);
        block.close();
    }

    void free(int pageId, int pageOffset, int capacity, int pageClass) {
        YierdisNativeBlock block = blockAt(pageId, pageOffset, capacity, pageClass, Math.max(1, capacity));
        block.close();
    }

    private YierdisNativeBlock allocateSmall(int requestedBytes) {
        YierdisNativeSizeClass sizeClass = YierdisNativeSizeClass.forSize(requestedBytes);
        SmallPage page = nonFullHeads[sizeClass.ordinal()];
        if (page == null) {
            page = newSmallPage(sizeClass);
        }
        if (page.inEmptyList) {
            unlinkEmpty(page);
        }
        int offset = page.popFreeOffset();
        freeBlocksByClass[sizeClass.ordinal()]--;
        smallFreeBytes -= sizeClass.bytes();
        page.liveBlocks++;
        if (page.freeCount == 0) {
            unlinkNonFull(page);
        }
        usedBytes += sizeClass.bytes();
        return new YierdisNativeBlock(
                this,
                page,
                page.region,
                offset,
                requestedBytes,
                sizeClass.bytes(),
                page.pageId,
                offset,
                1,
                YierdisNativePageClass.SMALL,
                sizeClass
        );
    }

    private YierdisNativeBlock allocateSpan(int requestedBytes) {
        int pageCount = pagesFor(requestedBytes);
        int capacity = Math.multiplyExact(pageCount, PAGE_BYTES);
        YierdisNativePageClass pageClass = requestedBytes <= MEDIUM_MAX_BYTES
                ? YierdisNativePageClass.MEDIUM_SPAN
                : YierdisNativePageClass.LARGE_SPAN;
        YierdisFfmRegion region = runtime.allocateRegion(pageClass.name().toLowerCase(), capacity);
        SpanAllocation span = new SpanAllocation(
                nextPageSequence++,
                pageCount,
                pageClass,
                region,
                capacity
        );
        span.pageId = pageDirectory.add(span);
        retainedPageHeapBytes = MemoryUsageSnapshot.addSaturating(retainedPageHeapBytes, spanHeapBytes());
        linkLiveSpan(span);
        committedBytes += capacity;
        usedBytes += capacity;
        liveSpanDescriptors++;
        if (pageClass == YierdisNativePageClass.MEDIUM_SPAN) {
            liveMediumSpanPages += pageCount;
        } else {
            liveLargeSpanPages += pageCount;
        }
        return new YierdisNativeBlock(
                this,
                span,
                region,
                0,
                requestedBytes,
                capacity,
                span.pageId,
                0,
                pageCount,
                pageClass,
                null
        );
    }

    private SmallPage newSmallPage(YierdisNativeSizeClass sizeClass) {
        YierdisFfmRegion region = runtime.allocateRegion("native-small-page", PAGE_BYTES);
        SmallPage page = new SmallPage(nextPageSequence++, sizeClass, region);
        page.pageId = pageDirectory.add(page);
        retainedPageHeapBytes = MemoryUsageSnapshot.addSaturating(
                retainedPageHeapBytes,
                smallPageHeapBytes(page)
        );
        linkLiveSmall(page);
        linkNonFull(page);
        committedBytes += PAGE_BYTES;
        liveSmallPages++;
        freeBlocksByClass[sizeClass.ordinal()] += page.freeCount;
        smallFreeBytes += (long) page.freeCount * sizeClass.bytes();
        return page;
    }

    private void freeSmall(YierdisNativeBlock block, SmallPage page) {
        if (page.closed) {
            return;
        }
        long next = usedBytes - block.capacity();
        if (next < 0 || page.liveBlocks <= 0) {
            throw new IllegalStateException("native page allocator accounting underflow");
        }
        boolean wasFull = page.freeCount == 0;
        usedBytes = next;
        page.liveBlocks--;
        page.pushFreeOffset(block.pageOffset());
        freeBlocksByClass[page.sizeClass.ordinal()]++;
        smallFreeBytes += page.sizeClass.bytes();
        if (wasFull) {
            linkNonFull(page);
        }
        if (page.liveBlocks == 0) {
            SmallPage oldWarmPage = emptyByClass[page.sizeClass.ordinal()];
            if (oldWarmPage != null && oldWarmPage != page) {
                if (createdInActiveScope(page) && !createdInActiveScope(oldWarmPage)) {
                    // abort 只能回收本 scope 新建的页，不能让临时 warm page 淘汰命令前的基线页。
                    closeSmallPage(page);
                    return;
                }
                closeSmallPage(oldWarmPage);
            }
            linkEmpty(page);
        }
    }

    private void freeSpan(YierdisNativeBlock block, SpanAllocation span) {
        if (span.closed) {
            return;
        }
        long nextUsed = usedBytes - block.capacity();
        long nextCommitted = committedBytes - block.capacity();
        if (nextUsed < 0 || nextCommitted < 0) {
            throw new IllegalStateException("native page allocator accounting underflow");
        }
        usedBytes = nextUsed;
        committedBytes = nextCommitted;
        unlinkLiveSpan(span);
        pageDirectory.remove(span.pageId, span, !allocationScopeAbortInProgress);
        subtractRetainedPageHeapBytes(spanHeapBytes());
        liveSpanDescriptors--;
        if (span.pageClass == YierdisNativePageClass.MEDIUM_SPAN) {
            liveMediumSpanPages -= span.pageCount;
        } else {
            liveLargeSpanPages -= span.pageCount;
        }
        span.closed = true;
        span.region.close();
    }

    private YierdisNativeBlock blockAt(
            int pageId,
            int pageOffset,
            int capacity,
            int pageClassOrdinal,
            int requestedBytes
    ) {
        if (pageId <= 0) {
            throw new IllegalStateException("invalid native page id: " + pageId);
        }
        if (pageOffset < 0 || capacity <= 0) {
            throw new IllegalStateException("invalid native block location");
        }
        if (pageClassOrdinal < 0 || pageClassOrdinal >= PAGE_CLASSES.length) {
            throw new IllegalStateException("invalid native page class: " + pageClassOrdinal);
        }
        YierdisNativePageClass pageClass = PAGE_CLASSES[pageClassOrdinal];
        Object entry = pageDirectory.get(pageId);
        if (entry instanceof SmallPage page) {
            if (page.closed || pageClass != YierdisNativePageClass.SMALL) {
                throw new IllegalStateException("native small page location is not live");
            }
            if (capacity != page.sizeClass.bytes()
                    || pageOffset % capacity != 0
                    || pageOffset > PAGE_BYTES - capacity) {
                throw new IllegalStateException("native small block location mismatch");
            }
            return new YierdisNativeBlock(
                    this,
                    page,
                    page.region,
                    pageOffset,
                    requestedBytes,
                    capacity,
                    pageId,
                    pageOffset,
                    1,
                    pageClass,
                    page.sizeClass
            );
        }
        if (entry instanceof SpanAllocation span) {
            if (span.closed || pageClass != span.pageClass || pageOffset != 0 || capacity != span.capacity) {
                throw new IllegalStateException("native span location mismatch");
            }
            return new YierdisNativeBlock(
                    this,
                    span,
                    span.region,
                    0,
                    requestedBytes,
                    capacity,
                    pageId,
                    0,
                    span.pageCount,
                    pageClass,
                    null
            );
        }
        throw new IllegalStateException("unknown or closed native page id: " + pageId);
    }

    private void closeSmallPage(SmallPage page) {
        if (page.closed || page.liveBlocks != 0) {
            throw new IllegalStateException("only an empty live small page can be closed");
        }
        unlinkEmpty(page);
        unlinkNonFull(page);
        unlinkLiveSmall(page);
        pageDirectory.remove(page.pageId, page, !allocationScopeAbortInProgress);
        subtractRetainedPageHeapBytes(smallPageHeapBytes(page));
        committedBytes -= PAGE_BYTES;
        smallFreeBytes -= (long) page.freeCount * page.sizeClass.bytes();
        freeBlocksByClass[page.sizeClass.ordinal()] -= page.freeCount;
        liveSmallPages--;
        page.closed = true;
        page.region.close();
    }

    private RuntimeException closeSmallPageOnShutdown(SmallPage page, RuntimeException failure) {
        unlinkEmptyIfPresent(page);
        unlinkNonFullIfPresent(page);
        unlinkLiveSmall(page);
        pageDirectory.remove(page.pageId, page);
        subtractRetainedPageHeapBytes(smallPageHeapBytes(page));
        page.closed = true;
        return closeRegion(page.region, failure);
    }

    private RuntimeException closeSpanOnShutdown(SpanAllocation span, RuntimeException failure) {
        unlinkLiveSpan(span);
        pageDirectory.remove(span.pageId, span);
        subtractRetainedPageHeapBytes(spanHeapBytes());
        span.closed = true;
        return closeRegion(span.region, failure);
    }

    private void linkLiveSmall(SmallPage page) {
        page.liveNext = liveSmallHead;
        if (liveSmallHead != null) {
            liveSmallHead.livePrev = page;
        }
        liveSmallHead = page;
    }

    private void unlinkLiveSmall(SmallPage page) {
        if (page.livePrev == null) {
            liveSmallHead = page.liveNext;
        } else {
            page.livePrev.liveNext = page.liveNext;
        }
        if (page.liveNext != null) {
            page.liveNext.livePrev = page.livePrev;
        }
        page.livePrev = null;
        page.liveNext = null;
    }

    private void linkNonFull(SmallPage page) {
        int index = page.sizeClass.ordinal();
        if (page.inNonFullList) {
            return;
        }
        page.nonFullNext = nonFullHeads[index];
        if (page.nonFullNext != null) {
            page.nonFullNext.nonFullPrev = page;
        }
        nonFullHeads[index] = page;
        page.inNonFullList = true;
    }

    private void unlinkNonFull(SmallPage page) {
        if (!page.inNonFullList) {
            throw new IllegalStateException("page is not in non-full list");
        }
        unlinkNonFullIfPresent(page);
    }

    private void unlinkNonFullIfPresent(SmallPage page) {
        if (!page.inNonFullList) {
            return;
        }
        int index = page.sizeClass.ordinal();
        if (page.nonFullPrev == null) {
            nonFullHeads[index] = page.nonFullNext;
        } else {
            page.nonFullPrev.nonFullNext = page.nonFullNext;
        }
        if (page.nonFullNext != null) {
            page.nonFullNext.nonFullPrev = page.nonFullPrev;
        }
        page.nonFullPrev = null;
        page.nonFullNext = null;
        page.inNonFullList = false;
    }

    private void linkEmpty(SmallPage page) {
        if (page.inEmptyList) {
            return;
        }
        int index = page.sizeClass.ordinal();
        emptyByClass[index] = page;
        page.emptyNext = emptyHead;
        if (emptyHead != null) {
            emptyHead.emptyPrev = page;
        }
        emptyHead = page;
        page.inEmptyList = true;
        emptySmallPages++;
    }

    private void unlinkEmpty(SmallPage page) {
        if (!page.inEmptyList) {
            throw new IllegalStateException("page is not in empty list");
        }
        unlinkEmptyIfPresent(page);
    }

    private void unlinkEmptyIfPresent(SmallPage page) {
        if (!page.inEmptyList) {
            return;
        }
        if (page.emptyPrev == null) {
            emptyHead = page.emptyNext;
        } else {
            page.emptyPrev.emptyNext = page.emptyNext;
        }
        if (page.emptyNext != null) {
            page.emptyNext.emptyPrev = page.emptyPrev;
        }
        int index = page.sizeClass.ordinal();
        if (emptyByClass[index] == page) {
            emptyByClass[index] = null;
        }
        page.emptyPrev = null;
        page.emptyNext = null;
        page.inEmptyList = false;
        emptySmallPages--;
    }

    private void linkLiveSpan(SpanAllocation span) {
        span.liveNext = liveSpanHead;
        if (liveSpanHead != null) {
            liveSpanHead.livePrev = span;
        }
        liveSpanHead = span;
    }

    private void unlinkLiveSpan(SpanAllocation span) {
        if (span.livePrev == null) {
            liveSpanHead = span.liveNext;
        } else {
            span.livePrev.liveNext = span.liveNext;
        }
        if (span.liveNext != null) {
            span.liveNext.livePrev = span.livePrev;
        }
        span.livePrev = null;
        span.liveNext = null;
    }

    private static int pagesFor(int bytes) {
        long pages = ((long) bytes + PAGE_BYTES - 1L) / PAGE_BYTES;
        if (pages <= 0 || pages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("allocation is too large: " + bytes);
        }
        return (int) pages;
    }

    private static long elapsedNanos(long startedNanos) {
        return Math.max(0L, System.nanoTime() - startedNanos);
    }

    private boolean createdInActiveScope(SmallPage page) {
        return activeAllocationScope != null
                && page.creationSequence >= activeAllocationScope.nextPageSequence;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native page allocator is closed");
        }
    }

    private static long smallPageHeapBytes(SmallPage page) {
        return SMALL_PAGE_HEAP_BYTES + REGION_WRAPPER_HEAP_BYTES + (long) page.freeOffsets.length * Integer.BYTES;
    }

    private static long spanHeapBytes() {
        return SPAN_HEAP_BYTES + REGION_WRAPPER_HEAP_BYTES;
    }

    private void subtractRetainedPageHeapBytes(long bytes) {
        if (bytes < 0L || retainedPageHeapBytes < bytes) {
            throw new IllegalStateException("native page heap accounting underflow");
        }
        retainedPageHeapBytes -= bytes;
    }

    private static RuntimeException closeRegion(YierdisFfmRegion region, RuntimeException failure) {
        try {
            region.close();
            return failure;
        } catch (RuntimeException e) {
            if (failure == null) {
                return e;
            }
            failure.addSuppressed(e);
            return failure;
        }
    }

    record PageGrowth(long heapEstimatedBytes, long nativeDataCommittedBytes) {
    }

    static final class AllocationScopeCheckpoint {
        private SmallPage smallPageHead;
        private SpanAllocation spanHead;
        private final long nextPageSequence;
        private YierdisNativePageDirectory.AllocationScopeCheckpoint directoryCheckpoint;

        private AllocationScopeCheckpoint(
                SmallPage smallPageHead,
                SpanAllocation spanHead,
                long nextPageSequence,
                YierdisNativePageDirectory.AllocationScopeCheckpoint directoryCheckpoint
        ) {
            this.smallPageHead = smallPageHead;
            this.spanHead = spanHead;
            this.nextPageSequence = nextPageSequence;
            this.directoryCheckpoint = directoryCheckpoint;
        }

        long heapEstimatedBytes() {
            return ALLOCATION_SCOPE_CHECKPOINT_HEAP_BYTES
                    + (directoryCheckpoint == null ? 0L : directoryCheckpoint.heapEstimatedBytes());
        }

        private void releaseReferences() {
            smallPageHead = null;
            spanHead = null;
            directoryCheckpoint = null;
        }
    }

    private static final class SmallPage {
        private int pageId;
        private final long creationSequence;
        private final YierdisNativeSizeClass sizeClass;
        private final YierdisFfmRegion region;
        private final int[] freeOffsets;
        private int freeCount;
        private int liveBlocks;
        private boolean closed;
        private boolean inNonFullList;
        private boolean inEmptyList;
        private SmallPage livePrev;
        private SmallPage liveNext;
        private SmallPage nonFullPrev;
        private SmallPage nonFullNext;
        private SmallPage emptyPrev;
        private SmallPage emptyNext;

        private SmallPage(
                long creationSequence,
                YierdisNativeSizeClass sizeClass,
                YierdisFfmRegion region
        ) {
            this.creationSequence = creationSequence;
            this.sizeClass = Objects.requireNonNull(sizeClass, "sizeClass");
            this.region = Objects.requireNonNull(region, "region");
            int blockCount = PAGE_BYTES / sizeClass.bytes();
            this.freeOffsets = new int[blockCount];
            for (int i = blockCount - 1; i >= 0; i--) {
                freeOffsets[freeCount++] = i * sizeClass.bytes();
            }
        }

        private int popFreeOffset() {
            if (freeCount <= 0) {
                throw new IllegalStateException("small page has no free block");
            }
            return freeOffsets[--freeCount];
        }

        private void pushFreeOffset(int offset) {
            if (freeCount >= freeOffsets.length) {
                throw new IllegalStateException("small page free stack overflow");
            }
            freeOffsets[freeCount++] = offset;
        }
    }

    private static final class SpanAllocation {
        private int pageId;
        private final long creationSequence;
        private final int pageCount;
        private final YierdisNativePageClass pageClass;
        private final YierdisFfmRegion region;
        private final int capacity;
        private boolean closed;
        private SpanAllocation livePrev;
        private SpanAllocation liveNext;

        private SpanAllocation(
                long creationSequence,
                int pageCount,
                YierdisNativePageClass pageClass,
                YierdisFfmRegion region,
                int capacity
        ) {
            this.creationSequence = creationSequence;
            this.pageCount = pageCount;
            this.pageClass = Objects.requireNonNull(pageClass, "pageClass");
            this.region = Objects.requireNonNull(region, "region");
            this.capacity = capacity;
        }
    }
}
