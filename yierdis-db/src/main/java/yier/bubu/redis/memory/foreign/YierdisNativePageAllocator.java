package yier.bubu.redis.memory.foreign;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;

final class YierdisNativePageAllocator
        implements AutoCloseable, YierdisNativeObjectTable.CapacityResolver {
    public static final int PAGE_BYTES = 64 * 1024;
    private static final int MEDIUM_MAX_BYTES = 1024 * 1024;
    private static final long SMALL_PAGE_HEAP_BYTES = 96L;
    private static final long SPAN_HEAP_BYTES = 72L;
    private static final long REGION_WRAPPER_HEAP_BYTES = 128L;
    private static final long PAGE_REGISTRY_BASE_HEAP_BYTES = 128L;
    private static final long PAGE_ID_ENTRY_HEAP_BYTES = 64L;
    private static final long ALLOCATION_SCOPE_CHECKPOINT_HEAP_BYTES = 48L;
    private static final YierdisNativePageClass[] PAGE_CLASSES = YierdisNativePageClass.values();

    private final YierdisFfmMemoryRuntime runtime;
    private final NavigableMap<Integer, PageAllocation> pagesById = new TreeMap<>();
    private final NavigableSet<Integer> reusablePageIds = new TreeSet<>();
    // 按 size class 只索引还有空闲块的页，以及已经空掉的页。分配和 warm-page 淘汰不再扫整个 registry。
    private final NavigableMap<Integer, SmallPage>[] reusableSmallPages = newPageIndexes();
    private final NavigableMap<Integer, SmallPage>[] emptySmallPages = newPageIndexes();
    private final long[] reusableSmallBlocks = new long[YierdisNativeSizeClass.count()];

    private boolean closed;
    private int nextPageId = 1;
    private long nextCreationSequence;
    private AllocationScopeCheckpoint activeAllocationScope;

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
        Map.Entry<Integer, PageAllocation> entry = pagesById.firstEntry();
        while (entry != null) {
            int pageId = entry.getKey();
            PageAllocation allocation = entry.getValue();
            if (allocation instanceof SmallPage page && page.liveBlocks == 0) {
                if (inspected >= budget.maxInspectedUnits()) {
                    return reclaimResult(
                            inspected,
                            reclaimedUnits,
                            reclaimedBytes,
                            MemoryReclaimResult.StopReason.INSPECTION_LIMIT
                    );
                }
                if (elapsedNanos(startedNanos) >= budget.timeLimitNanos()) {
                    return reclaimResult(
                            inspected,
                            reclaimedUnits,
                            reclaimedBytes,
                            MemoryReclaimResult.StopReason.TIME_LIMIT
                    );
                }
                inspected++;
                if (reclaimedBytes > budget.maxReclaimedBytes() - PAGE_BYTES) {
                    return reclaimResult(
                            inspected,
                            reclaimedUnits,
                            reclaimedBytes,
                            MemoryReclaimResult.StopReason.BYTE_LIMIT
                    );
                }
                closeSmallPage(page);
                reclaimedUnits++;
                reclaimedBytes += PAGE_BYTES;
            } else if (elapsedNanos(startedNanos) >= budget.timeLimitNanos()) {
                return reclaimResult(
                        inspected,
                        reclaimedUnits,
                        reclaimedBytes,
                        MemoryReclaimResult.StopReason.TIME_LIMIT
                );
            }
            entry = pagesById.higherEntry(pageId);
        }
        return reclaimResult(
                inspected,
                reclaimedUnits,
                reclaimedBytes,
                MemoryReclaimResult.StopReason.COMPLETE
        );
    }

    public YierdisNativePageAllocatorStats stats() {
        PageSummary summary = summarizePages();
        return new YierdisNativePageAllocatorStats(
                summary.committedBytes(),
                summary.usedBytes(),
                summary.committedBytes() - summary.usedBytes(),
                summary.liveSmallPages(),
                summary.liveMediumPages(),
                summary.liveLargePages(),
                summary.smallFreeBytes(),
                summary.emptySmallPages(),
                pagesById.size(),
                summary.liveSpanDescriptors(),
                pageRegistryHeapEstimatedBytes()
        );
    }

    PageGrowth estimateAdditionalGrowth(int... requestedBytes) {
        return estimateGrowth(requestedBytes);
    }

    private PageGrowth estimateGrowth(int... requestedBytes) {
        Objects.requireNonNull(requestedBytes, "requestedBytes");
        long[] availableBlocks = availableSmallBlocks();
        int additionalEntries = 0;
        int reusableIndexEntries = 0;
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
                // 小页至少两块。拿走第一块后该页仍在非满页索引中，堆估算要算上这一项。
                // 估算不回头撤销。真正分配时若后续请求把该页用满，索引会摘掉它；多算的 64 字节只让预留偏大。
                reusableIndexEntries++;
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
                saturatingMultiply((long) additionalEntries + reusableIndexEntries, PAGE_ID_ENTRY_HEAP_BYTES)
        );
        return new PageGrowth(heapBytes, dataBytes);
    }

    long heapEstimatedBytes() {
        return MemoryUsageSnapshot.addSaturating(
                pageRegistryHeapEstimatedBytes(),
                summarizePages().descriptorHeapBytes()
        );
    }

    AllocationScopeCheckpoint beginAllocationScope() {
        ensureOpen();
        if (activeAllocationScope != null) {
            throw new IllegalStateException("native page allocation scope is already active");
        }
        AllocationScopeCheckpoint checkpoint = new AllocationScopeCheckpoint(
                nextCreationSequence,
                nextPageId,
                new TreeSet<>(reusablePageIds)
        );
        activeAllocationScope = checkpoint;
        return checkpoint;
    }

    long allocationScopeCheckpointHeapEstimatedBytes() {
        ensureOpen();
        return allocationScopeCheckpointHeapEstimatedBytes(reusablePageIds.size());
    }

    void promoteAllocationScope(AllocationScopeCheckpoint checkpoint) {
        if (activeAllocationScope == checkpoint) {
            activeAllocationScope = null;
            checkpoint.releaseReferences();
        }
    }

    void beginAllocationScopeAbort(AllocationScopeCheckpoint checkpoint) {
        ensureOpen();
        if (activeAllocationScope != checkpoint) {
            throw new IllegalStateException("native page allocation scope is not active");
        }
    }

    void restoreAllocationScope(AllocationScopeCheckpoint checkpoint) {
        ensureOpen();
        if (activeAllocationScope != checkpoint) {
            throw new IllegalStateException("native page allocation scope is not active");
        }
        try {
            List<SmallPage> createdEmptyPages = new ArrayList<>();
            for (PageAllocation allocation : pagesById.values()) {
                if (allocation.creationSequence < checkpoint.creationSequence) {
                    continue;
                }
                if (!(allocation instanceof SmallPage page) || page.liveBlocks != 0) {
                    throw new IllegalStateException("allocation scope left a live native page");
                }
                createdEmptyPages.add(page);
            }
            for (SmallPage page : createdEmptyPages) {
                closeSmallPage(page);
            }

            // scope 新页已全部消失，此时恢复快照才不会让可复用 ID 指向仍存活的描述符。
            reusablePageIds.clear();
            reusablePageIds.addAll(checkpoint.reusablePageIds);
            nextPageId = checkpoint.nextPageId;
            nextCreationSequence = checkpoint.creationSequence;
        } finally {
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
        while (!pagesById.isEmpty()) {
            PageAllocation allocation = pagesById.pollFirstEntry().getValue();
            allocation.closed = true;
            failure = closeRegion(allocation.region, failure);
        }
        reusablePageIds.clear();
        clearSmallPageIndexes();
        if (activeAllocationScope != null) {
            activeAllocationScope.releaseReferences();
            activeAllocationScope = null;
        }
        nextPageId = 1;
        nextCreationSequence = 0L;
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
            freeSpan(span);
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
        PageAllocation entry = pagesById.get(pageId);
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
                meta.pageId(),
                Math.toIntExact(meta.pageOffset()),
                meta.capacity(),
                meta.pageClass()
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
        YierdisNativeBlock block = blockAt(pageId, pageOffset, capacity, pageClass);
        block.close();
    }

    private YierdisNativeBlock allocateSmall(int requestedBytes) {
        YierdisNativeSizeClass sizeClass = YierdisNativeSizeClass.forSize(requestedBytes);
        SmallPage page = findNonFullPage(sizeClass);
        if (page == null) {
            page = newSmallPage(sizeClass);
        }
        int offset = page.popFreeOffset();
        page.liveBlocks++;
        reusableSmallBlocks[sizeClass.ordinal()]--;
        if (page.freeCount == 0) {
            unindexReusable(page);
        }
        if (page.liveBlocks == 1) {
            unindexEmpty(page);
        }
        return new YierdisNativeBlock(
                this,
                page,
                page.region,
                offset,
                sizeClass.bytes(),
                page.pageId,
                offset,
                YierdisNativePageClass.SMALL
        );
    }

    private YierdisNativeBlock allocateSpan(int requestedBytes) {
        int pageCount = pagesFor(requestedBytes);
        int capacity = Math.multiplyExact(pageCount, PAGE_BYTES);
        YierdisNativePageClass pageClass = requestedBytes <= MEDIUM_MAX_BYTES
                ? YierdisNativePageClass.MEDIUM_SPAN
                : YierdisNativePageClass.LARGE_SPAN;
        int pageId = claimPageId();
        YierdisFfmRegion region = null;
        SpanAllocation span = null;
        try {
            region = runtime.allocateRegion(pageClass.name().toLowerCase(), capacity);
            span = new SpanAllocation(
                    pageId,
                    nextCreationSequence,
                    pageCount,
                    pageClass,
                    region,
                    capacity
            );
            registerPage(pageId, span);
            nextCreationSequence++;
        } catch (RuntimeException | Error failure) {
            releaseFailedAllocation(pageId, span, region, failure);
            throw failure;
        }
        return new YierdisNativeBlock(
                this,
                span,
                region,
                0,
                capacity,
                pageId,
                0,
                pageClass
        );
    }

    private SmallPage newSmallPage(YierdisNativeSizeClass sizeClass) {
        int pageId = claimPageId();
        YierdisFfmRegion region = null;
        SmallPage page = null;
        boolean reusableIndexed = false;
        boolean emptyIndexed = false;
        try {
            region = runtime.allocateRegion("native-small-page", PAGE_BYTES);
            page = new SmallPage(pageId, nextCreationSequence, sizeClass, region);
            registerPage(pageId, page);
            nextCreationSequence++;
            indexReusable(page);
            reusableIndexed = true;
            indexEmpty(page);
            emptyIndexed = true;
            reusableSmallBlocks[sizeClass.ordinal()] += page.freeCount;
            return page;
        } catch (RuntimeException | Error failure) {
            if (reusableIndexed) {
                reusableSmallPages[sizeClass.ordinal()].remove(page.pageId);
            }
            if (emptyIndexed) {
                emptySmallPages[sizeClass.ordinal()].remove(page.pageId);
            }
            releaseFailedAllocation(pageId, page, region, failure);
            throw failure;
        }
    }

    private void freeSmall(YierdisNativeBlock block, SmallPage page) {
        if (page.closed) {
            return;
        }
        if (pagesById.get(page.pageId) != page || page.liveBlocks <= 0) {
            throw new IllegalStateException("native page allocator state mismatch");
        }
        page.liveBlocks--;
        boolean wasFull = page.freeCount == 0;
        page.pushFreeOffset(block.pageOffset());
        reusableSmallBlocks[page.sizeClass.ordinal()]++;
        if (wasFull) {
            indexReusable(page);
        }
        if (page.liveBlocks != 0) {
            return;
        }

        indexEmpty(page);
        SmallPage warmPage = findWarmPage(page.sizeClass, page);
        if (warmPage == null) {
            return;
        }
        if (createdInActiveScope(page) && !createdInActiveScope(warmPage)) {
            // abort 只回收 scope 新建页，临时 warm page 不能淘汰命令前的基线页。
            closeSmallPage(page);
            return;
        }
        closeSmallPage(warmPage);
    }

    private void freeSpan(SpanAllocation span) {
        if (span.closed) {
            return;
        }
        removePage(span.pageId, span, true);
        span.closed = true;
        span.region.close();
    }

    private YierdisNativeBlock blockAt(
            int pageId,
            int pageOffset,
            int capacity,
            int pageClassOrdinal
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
        PageAllocation entry = pagesById.get(pageId);
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
                    capacity,
                    pageId,
                    pageOffset,
                    pageClass
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
                    capacity,
                    pageId,
                    0,
                    pageClass
            );
        }
        throw new IllegalStateException("unknown or closed native page id: " + pageId);
    }

    private SmallPage findNonFullPage(YierdisNativeSizeClass sizeClass) {
        Map.Entry<Integer, SmallPage> entry = reusableSmallPages[sizeClass.ordinal()].firstEntry();
        return entry == null ? null : entry.getValue();
    }

    private SmallPage findWarmPage(YierdisNativeSizeClass sizeClass, SmallPage excluded) {
        NavigableMap<Integer, SmallPage> emptyPages = emptySmallPages[sizeClass.ordinal()];
        Map.Entry<Integer, SmallPage> first = emptyPages.firstEntry();
        if (first == null) {
            return null;
        }
        if (first.getValue() != excluded) {
            return first.getValue();
        }
        Map.Entry<Integer, SmallPage> next = emptyPages.higherEntry(first.getKey());
        return next == null ? null : next.getValue();
    }

    private long[] availableSmallBlocks() {
        return reusableSmallBlocks.clone();
    }

    private void closeSmallPage(SmallPage page) {
        if (page.closed || page.liveBlocks != 0) {
            throw new IllegalStateException("only an empty live small page can be closed");
        }
        reusableSmallBlocks[page.sizeClass.ordinal()] -= page.freeCount;
        unindexReusable(page);
        unindexEmpty(page);
        removePage(page.pageId, page, true);
        page.closed = true;
        page.region.close();
    }

    private int claimPageId() {
        // 复用集合只接收已脱离 registry 的 ID；旧句柄仍由 object-table generation 判为 stale。
        Integer reusable = reusablePageIds.pollFirst();
        if (reusable != null) {
            return reusable;
        }
        if (nextPageId <= 0) {
            throw new NativeCapacityExceededException("native page id space exhausted");
        }
        int pageId = nextPageId;
        nextPageId = pageId == Integer.MAX_VALUE ? -1 : pageId + 1;
        return pageId;
    }

    private void registerPage(int pageId, PageAllocation allocation) {
        if (pagesById.putIfAbsent(pageId, allocation) != null) {
            throw new IllegalStateException("page id is already live: " + pageId);
        }
    }

    private void removePage(int pageId, PageAllocation expected, boolean recycleId) {
        if (pagesById.get(pageId) != expected) {
            throw new IllegalStateException("page id owner mismatch: " + pageId);
        }
        pagesById.remove(pageId);
        if (recycleId && !reusablePageIds.add(pageId)) {
            throw new IllegalStateException("page id is already reusable: " + pageId);
        }
    }

    private void releaseFailedAllocation(
            int pageId,
            PageAllocation allocation,
            YierdisFfmRegion region,
            Throwable failure
    ) {
        if (allocation != null && pagesById.get(pageId) == allocation) {
            pagesById.remove(pageId);
        }
        if (region != null) {
            try {
                region.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
        reusablePageIds.add(pageId);
    }

    private PageSummary summarizePages() {
        long committedBytes = 0L;
        long usedBytes = 0L;
        long smallFreeBytes = 0L;
        long liveSmallPages = 0L;
        long liveMediumPages = 0L;
        long liveLargePages = 0L;
        long emptySmallPages = 0L;
        long liveSpanDescriptors = 0L;
        long descriptorHeapBytes = 0L;
        for (PageAllocation allocation : pagesById.values()) {
            if (allocation instanceof SmallPage page) {
                committedBytes = MemoryUsageSnapshot.addSaturating(committedBytes, PAGE_BYTES);
                usedBytes = MemoryUsageSnapshot.addSaturating(
                        usedBytes,
                        (long) page.liveBlocks * page.sizeClass.bytes()
                );
                smallFreeBytes = MemoryUsageSnapshot.addSaturating(
                        smallFreeBytes,
                        (long) page.freeCount * page.sizeClass.bytes()
                );
                liveSmallPages++;
                if (page.liveBlocks == 0) {
                    emptySmallPages++;
                }
                descriptorHeapBytes = MemoryUsageSnapshot.addSaturating(
                        descriptorHeapBytes,
                        smallPageHeapBytes(page)
                );
            } else if (allocation instanceof SpanAllocation span) {
                committedBytes = MemoryUsageSnapshot.addSaturating(committedBytes, span.capacity);
                usedBytes = MemoryUsageSnapshot.addSaturating(usedBytes, span.capacity);
                if (span.pageClass == YierdisNativePageClass.MEDIUM_SPAN) {
                    liveMediumPages += span.pageCount;
                } else {
                    liveLargePages += span.pageCount;
                }
                liveSpanDescriptors++;
                descriptorHeapBytes = MemoryUsageSnapshot.addSaturating(
                        descriptorHeapBytes,
                        spanHeapBytes()
                );
            }
        }
        return new PageSummary(
                committedBytes,
                usedBytes,
                smallFreeBytes,
                liveSmallPages,
                liveMediumPages,
                liveLargePages,
                emptySmallPages,
                liveSpanDescriptors,
                descriptorHeapBytes
        );
    }

    private long pageRegistryHeapEstimatedBytes() {
        long entryCount = (long) pagesById.size() + reusablePageIds.size();
        long indexedPages = 0L;
        for (NavigableMap<Integer, SmallPage> pages : reusableSmallPages) {
            indexedPages += pages.size();
        }
        for (NavigableMap<Integer, SmallPage> pages : emptySmallPages) {
            indexedPages += pages.size();
        }
        return MemoryUsageSnapshot.addSaturating(
                PAGE_REGISTRY_BASE_HEAP_BYTES + smallPageIndexBaseBytes(),
                saturatingMultiply(entryCount + indexedPages, PAGE_ID_ENTRY_HEAP_BYTES)
        );
    }

    private static long smallPageIndexBaseBytes() {
        return saturatingMultiply(YierdisNativeSizeClass.count() * 2L, 64L);
    }

    @SuppressWarnings("unchecked")
    private static NavigableMap<Integer, SmallPage>[] newPageIndexes() {
        NavigableMap<Integer, SmallPage>[] indexes = new NavigableMap[YierdisNativeSizeClass.count()];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = new TreeMap<>();
        }
        return indexes;
    }

    private void indexReusable(SmallPage page) {
        if (reusableSmallPages[page.sizeClass.ordinal()].putIfAbsent(page.pageId, page) != null) {
            throw new IllegalStateException("small page is already reusable: " + page.pageId);
        }
    }

    private void unindexReusable(SmallPage page) {
        if (reusableSmallPages[page.sizeClass.ordinal()].remove(page.pageId) == null) {
            throw new IllegalStateException("small page is not reusable: " + page.pageId);
        }
    }

    private void indexEmpty(SmallPage page) {
        if (emptySmallPages[page.sizeClass.ordinal()].putIfAbsent(page.pageId, page) != null) {
            throw new IllegalStateException("small page is already empty: " + page.pageId);
        }
    }

    private void unindexEmpty(SmallPage page) {
        if (emptySmallPages[page.sizeClass.ordinal()].remove(page.pageId) == null) {
            throw new IllegalStateException("small page is not empty: " + page.pageId);
        }
    }

    private void clearSmallPageIndexes() {
        for (NavigableMap<Integer, SmallPage> pages : reusableSmallPages) {
            pages.clear();
        }
        for (NavigableMap<Integer, SmallPage> pages : emptySmallPages) {
            pages.clear();
        }
        Arrays.fill(reusableSmallBlocks, 0L);
    }

    private boolean createdInActiveScope(SmallPage page) {
        return activeAllocationScope != null
                && page.creationSequence >= activeAllocationScope.creationSequence;
    }

    private static int pagesFor(int bytes) {
        long pages = ((long) bytes + PAGE_BYTES - 1L) / PAGE_BYTES;
        // 容量是 int。32768 个 64KiB 页等于 2GiB，放不进 int。
        // 向上取整后第一个越界请求是 2GiB - 65535，不是 2GiB + 1。
        if (pages <= 0 || pages > Integer.MAX_VALUE / PAGE_BYTES) {
            throw new IllegalArgumentException("allocation is too large: " + bytes);
        }
        return (int) pages;
    }

    private static long elapsedNanos(long startedNanos) {
        return Math.max(0L, System.nanoTime() - startedNanos);
    }

    private static MemoryReclaimResult reclaimResult(
            long inspected,
            long reclaimedUnits,
            long reclaimedBytes,
            MemoryReclaimResult.StopReason stopReason
    ) {
        return new MemoryReclaimResult(inspected, reclaimedUnits, reclaimedBytes, stopReason);
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

    private static long saturatingMultiply(long value, long multiplier) {
        if (value <= 0L || multiplier <= 0L) {
            return 0L;
        }
        return value > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : value * multiplier;
    }

    private static long allocationScopeCheckpointHeapEstimatedBytes(int reusableIdCount) {
        return MemoryUsageSnapshot.addSaturating(
                ALLOCATION_SCOPE_CHECKPOINT_HEAP_BYTES + PAGE_REGISTRY_BASE_HEAP_BYTES,
                saturatingMultiply(reusableIdCount, PAGE_ID_ENTRY_HEAP_BYTES)
        );
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
        private final long creationSequence;
        private final int nextPageId;
        private NavigableSet<Integer> reusablePageIds;

        private AllocationScopeCheckpoint(
                long creationSequence,
                int nextPageId,
                NavigableSet<Integer> reusablePageIds
        ) {
            this.creationSequence = creationSequence;
            this.nextPageId = nextPageId;
            this.reusablePageIds = reusablePageIds;
        }

        long heapEstimatedBytes() {
            return reusablePageIds == null
                    ? 0L
                    : allocationScopeCheckpointHeapEstimatedBytes(reusablePageIds.size());
        }

        private void releaseReferences() {
            reusablePageIds = null;
        }
    }

    private record PageSummary(
            long committedBytes,
            long usedBytes,
            long smallFreeBytes,
            long liveSmallPages,
            long liveMediumPages,
            long liveLargePages,
            long emptySmallPages,
            long liveSpanDescriptors,
            long descriptorHeapBytes
    ) {
    }

    private abstract static class PageAllocation {
        final int pageId;
        final long creationSequence;
        final YierdisFfmRegion region;
        boolean closed;

        private PageAllocation(int pageId, long creationSequence, YierdisFfmRegion region) {
            this.pageId = pageId;
            this.creationSequence = creationSequence;
            this.region = Objects.requireNonNull(region, "region");
        }
    }

    private static final class SmallPage extends PageAllocation {
        private final YierdisNativeSizeClass sizeClass;
        private final int[] freeOffsets;
        private int freeCount;
        private int liveBlocks;

        private SmallPage(
                int pageId,
                long creationSequence,
                YierdisNativeSizeClass sizeClass,
                YierdisFfmRegion region
        ) {
            super(pageId, creationSequence, region);
            this.sizeClass = Objects.requireNonNull(sizeClass, "sizeClass");
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

    private static final class SpanAllocation extends PageAllocation {
        private final int pageCount;
        private final YierdisNativePageClass pageClass;
        private final int capacity;

        private SpanAllocation(
                int pageId,
                long creationSequence,
                int pageCount,
                YierdisNativePageClass pageClass,
                YierdisFfmRegion region,
                int capacity
        ) {
            super(pageId, creationSequence, region);
            this.pageCount = pageCount;
            this.pageClass = Objects.requireNonNull(pageClass, "pageClass");
            this.capacity = capacity;
        }
    }
}
