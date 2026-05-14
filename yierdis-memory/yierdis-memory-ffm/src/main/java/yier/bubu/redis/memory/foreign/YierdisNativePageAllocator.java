package yier.bubu.redis.memory.foreign;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Objects;

public final class YierdisNativePageAllocator implements AutoCloseable {
    public static final int PAGE_BYTES = 64 * 1024;
    private static final int MEDIUM_MAX_BYTES = 1024 * 1024;

    private final YierdisFfmMemoryRuntime runtime;
    private final ArrayList<SmallPage> smallPages = new ArrayList<>();
    private final ArrayList<SpanAllocation> spans = new ArrayList<>();

    private boolean closed;
    private int nextPageId = 1;
    private long committedBytes;
    private long usedBytes;

    public YierdisNativePageAllocator(YierdisFfmMemoryRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    public synchronized YierdisNativeBlock allocate(int requestedBytes) {
        ensureOpen();
        if (requestedBytes <= 0) {
            throw new IllegalArgumentException("requestedBytes must be > 0");
        }
        if (requestedBytes <= YierdisNativeSizeClass.MAX_SMALL_BYTES) {
            return allocateSmall(requestedBytes);
        }
        return allocateSpan(requestedBytes);
    }

    public synchronized YierdisNativePageAllocatorStats stats() {
        long mediumPages = 0;
        long largePages = 0;
        for (int i = 0; i < spans.size(); i++) {
            SpanAllocation span = spans.get(i);
            if (span.closed) {
                continue;
            }
            if (span.pageClass == YierdisNativePageClass.MEDIUM_SPAN) {
                mediumPages += span.pageCount;
            } else if (span.pageClass == YierdisNativePageClass.LARGE_SPAN) {
                largePages += span.pageCount;
            }
        }
        return new YierdisNativePageAllocatorStats(
                committedBytes,
                usedBytes,
                committedBytes - usedBytes,
                smallPages.size(),
                mediumPages,
                largePages
        );
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        for (int i = 0; i < smallPages.size(); i++) {
            failure = closeRegion(smallPages.get(i).region, failure);
        }
        for (int i = 0; i < spans.size(); i++) {
            SpanAllocation span = spans.get(i);
            if (!span.closed) {
                span.closed = true;
                failure = closeRegion(span.region, failure);
            }
        }
        smallPages.clear();
        spans.clear();
        committedBytes = 0;
        usedBytes = 0;
        if (failure != null) {
            throw failure;
        }
    }

    synchronized void free(YierdisNativeBlock block) {
        if (closed) {
            return;
        }
        Object allocation = block.allocation();
        if (allocation instanceof SmallAllocation small) {
            freeSmall(block, small);
            return;
        }
        if (allocation instanceof SpanAllocation span) {
            freeSpan(block, span);
            return;
        }
        throw new IllegalStateException("unknown native block allocation");
    }

    private YierdisNativeBlock allocateSmall(int requestedBytes) {
        YierdisNativeSizeClass sizeClass = YierdisNativeSizeClass.forSize(requestedBytes);
        SmallPage page = findSmallPage(sizeClass);
        if (page == null) {
            page = newSmallPage(sizeClass);
        }

        int offset = page.freeOffsets.removeFirst();
        page.liveBlocks++;
        usedBytes += sizeClass.bytes();
        return new YierdisNativeBlock(
                this,
                new SmallAllocation(page),
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
        SpanAllocation span = new SpanAllocation(nextPageId, pageCount, pageClass, region, capacity);
        nextPageId += pageCount;
        spans.add(span);
        committedBytes += capacity;
        usedBytes += capacity;
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

    private SmallPage findSmallPage(YierdisNativeSizeClass sizeClass) {
        for (int i = 0; i < smallPages.size(); i++) {
            SmallPage page = smallPages.get(i);
            if (page.sizeClass == sizeClass && !page.freeOffsets.isEmpty()) {
                return page;
            }
        }
        return null;
    }

    private SmallPage newSmallPage(YierdisNativeSizeClass sizeClass) {
        YierdisFfmRegion region = runtime.allocateRegion("native-small-page", PAGE_BYTES);
        SmallPage page = new SmallPage(nextPageId++, sizeClass, region);
        int blockBytes = sizeClass.bytes();
        int blockCount = PAGE_BYTES / blockBytes;
        for (int i = 0; i < blockCount; i++) {
            page.freeOffsets.addLast(i * blockBytes);
        }
        smallPages.add(page);
        committedBytes += PAGE_BYTES;
        return page;
    }

    private void freeSmall(YierdisNativeBlock block, SmallAllocation allocation) {
        SmallPage page = allocation.page;
        if (page.closed) {
            return;
        }
        long next = usedBytes - block.capacity();
        if (next < 0) {
            throw new IllegalStateException("native page allocator accounting underflow");
        }
        usedBytes = next;
        page.liveBlocks--;
        page.freeOffsets.addFirst(block.pageOffset());
    }

    private void freeSpan(YierdisNativeBlock block, SpanAllocation span) {
        if (span.closed) {
            return;
        }
        span.closed = true;
        long nextUsed = usedBytes - block.capacity();
        long nextCommitted = committedBytes - block.capacity();
        if (nextUsed < 0 || nextCommitted < 0) {
            throw new IllegalStateException("native page allocator accounting underflow");
        }
        usedBytes = nextUsed;
        committedBytes = nextCommitted;
        span.region.close();
    }

    private static int pagesFor(int bytes) {
        long pages = ((long) bytes + PAGE_BYTES - 1L) / PAGE_BYTES;
        if (pages <= 0 || pages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("allocation is too large: " + bytes);
        }
        return (int) pages;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native page allocator is closed");
        }
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

    private static final class SmallPage {
        private final int pageId;
        private final YierdisNativeSizeClass sizeClass;
        private final YierdisFfmRegion region;
        private final ArrayDeque<Integer> freeOffsets = new ArrayDeque<>();
        private int liveBlocks;
        private boolean closed;

        private SmallPage(int pageId, YierdisNativeSizeClass sizeClass, YierdisFfmRegion region) {
            this.pageId = pageId;
            this.sizeClass = Objects.requireNonNull(sizeClass, "sizeClass");
            this.region = Objects.requireNonNull(region, "region");
        }
    }

    private record SmallAllocation(SmallPage page) {
        private SmallAllocation {
            Objects.requireNonNull(page, "page");
        }
    }

    private static final class SpanAllocation {
        private final int pageId;
        private final int pageCount;
        private final YierdisNativePageClass pageClass;
        private final YierdisFfmRegion region;
        private final int capacity;
        private boolean closed;

        private SpanAllocation(
                int pageId,
                int pageCount,
                YierdisNativePageClass pageClass,
                YierdisFfmRegion region,
                int capacity
        ) {
            this.pageId = pageId;
            this.pageCount = pageCount;
            this.pageClass = Objects.requireNonNull(pageClass, "pageClass");
            this.region = Objects.requireNonNull(region, "region");
            this.capacity = capacity;
        }
    }
}
