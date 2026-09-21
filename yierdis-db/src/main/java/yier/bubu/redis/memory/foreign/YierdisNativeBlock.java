package yier.bubu.redis.memory.foreign;

import java.util.Objects;

final class YierdisNativeBlock implements AutoCloseable {
    private final YierdisNativePageAllocator owner;
    private final Object allocation;
    private final YierdisFfmRegion region;
    private final int regionOffset;
    private final int capacity;
    private final int pageId;
    private final int pageOffset;
    private final YierdisNativePageClass pageClass;

    private boolean closed;

    YierdisNativeBlock(
            YierdisNativePageAllocator owner,
            Object allocation,
            YierdisFfmRegion region,
            int regionOffset,
            int capacity,
            int pageId,
            int pageOffset,
            YierdisNativePageClass pageClass
    ) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.allocation = Objects.requireNonNull(allocation, "allocation");
        this.region = Objects.requireNonNull(region, "region");
        if (regionOffset < 0) {
            throw new IllegalArgumentException("regionOffset must be >= 0");
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
        this.regionOffset = regionOffset;
        this.capacity = capacity;
        this.pageId = pageId;
        this.pageOffset = pageOffset;
        this.pageClass = Objects.requireNonNull(pageClass, "pageClass");
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

    public YierdisNativePageClass pageClass() {
        return pageClass;
    }

    public byte getByte(int index) {
        ensureOpen();
        checkRange(index, 1);
        return region.getByte(regionOffset + index);
    }

    public void setByte(int index, byte value) {
        ensureOpen();
        checkRange(index, 1);
        region.setByte(regionOffset + index, value);
    }

    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        ensureOpen();
        checkRange(index, len);
        region.getBytes(regionOffset + index, dst, dstOff, len);
    }

    public void setBytes(int index, byte[] src, int srcOff, int len) {
        ensureOpen();
        checkRange(index, len);
        region.setBytes(regionOffset + index, src, srcOff, len);
    }

    void copyTo(YierdisNativeBlock target, int length) {
        ensureOpen();
        Objects.requireNonNull(target, "target").ensureOpen();
        checkRange(0, length);
        target.checkRange(0, length);
        region.copyTo(regionOffset, target.region, target.regionOffset, length);
    }

    Object allocation() {
        return allocation;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        owner.free(this);
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
