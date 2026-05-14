package yier.bubu.redis.memory.foreign;

import java.util.Objects;

public final class YierdisNativeBlock implements AutoCloseable {
    private final YierdisNativePageAllocator owner;
    private final Object allocation;
    private final YierdisFfmRegion region;
    private final int regionOffset;
    private final int requestedBytes;
    private final int capacity;
    private final int pageId;
    private final int pageOffset;
    private final int pageCount;
    private final YierdisNativePageClass pageClass;
    private final YierdisNativeSizeClass sizeClass;

    private boolean closed;

    YierdisNativeBlock(
            YierdisNativePageAllocator owner,
            Object allocation,
            YierdisFfmRegion region,
            int regionOffset,
            int requestedBytes,
            int capacity,
            int pageId,
            int pageOffset,
            int pageCount,
            YierdisNativePageClass pageClass,
            YierdisNativeSizeClass sizeClass
    ) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.allocation = Objects.requireNonNull(allocation, "allocation");
        this.region = Objects.requireNonNull(region, "region");
        if (regionOffset < 0) {
            throw new IllegalArgumentException("regionOffset must be >= 0");
        }
        if (requestedBytes <= 0) {
            throw new IllegalArgumentException("requestedBytes must be > 0");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (pageId <= 0) {
            throw new IllegalArgumentException("pageId must be > 0");
        }
        if (pageOffset < 0) {
            throw new IllegalArgumentException("pageOffset must be >= 0");
        }
        if (pageCount <= 0) {
            throw new IllegalArgumentException("pageCount must be > 0");
        }
        this.regionOffset = regionOffset;
        this.requestedBytes = requestedBytes;
        this.capacity = capacity;
        this.pageId = pageId;
        this.pageOffset = pageOffset;
        this.pageCount = pageCount;
        this.pageClass = Objects.requireNonNull(pageClass, "pageClass");
        this.sizeClass = sizeClass;
    }

    public int requestedBytes() {
        return requestedBytes;
    }

    public int capacity() {
        return capacity;
    }

    public int pageId() {
        return pageId;
    }

    public int pageOffset() {
        return pageOffset;
    }

    public int pageCount() {
        return pageCount;
    }

    public YierdisNativePageClass pageClass() {
        return pageClass;
    }

    public YierdisNativeSizeClass sizeClass() {
        return sizeClass;
    }

    public byte getByte(int index) {
        ensureOpen();
        checkRange(index, 1);
        return YierdisFfmAccess.getByte(region.span(regionOffset, capacity), index);
    }

    public void setByte(int index, byte value) {
        ensureOpen();
        checkRange(index, 1);
        YierdisFfmAccess.setByte(region.span(regionOffset, capacity), index, value);
    }

    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        ensureOpen();
        checkRange(index, len);
        YierdisFfmAccess.getBytes(region.span(regionOffset, capacity), index, dst, dstOff, len);
    }

    public void setBytes(int index, byte[] src, int srcOff, int len) {
        ensureOpen();
        checkRange(index, len);
        YierdisFfmAccess.setBytes(region.span(regionOffset, capacity), index, src, srcOff, len);
    }

    YierdisFfmSpan span() {
        ensureOpen();
        return region.span(regionOffset, capacity);
    }

    Object allocation() {
        return allocation;
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        owner.free(this);
    }

    void closeFromOwner() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native block is closed");
        }
    }

    private void checkRange(int index, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (index < 0 || index > capacity - len) {
            throw new IndexOutOfBoundsException();
        }
    }
}
