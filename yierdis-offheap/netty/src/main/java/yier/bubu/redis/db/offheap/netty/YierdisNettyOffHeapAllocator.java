package yier.bubu.redis.db.offheap.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBuf;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;

public final class YierdisNettyOffHeapAllocator implements YierdisOffHeapAllocator {
    private final long maxBytes;

    private boolean closed;
    private long usedBytes;

    public YierdisNettyOffHeapAllocator(long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0");
        }
        this.maxBytes = maxBytes;
    }

    @Override
    public YierdisOffHeapBuf allocate(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (closed) {
            throw new IllegalStateException("allocator is closed");
        }

        long next = usedBytes + capacity;
        if (maxBytes > 0 && next > maxBytes) {
            throw new YierdisOffHeapOutOfMemoryException("off-heap memory limit exceeded");
        }

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(capacity, capacity);
        if (!buf.isDirect()) {
            buf.release();
            throw new IllegalStateException("expected direct ByteBuf");
        }

        usedBytes = next;
        return new YierdisNettyOffHeapBuf(this, buf, capacity);
    }

    @Override
    public long usedBytes() {
        return usedBytes;
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    @Override
    public YierdisOffHeapBackend backend() {
        return YierdisOffHeapBackend.NETTY;
    }

    @Override
    public void close() {
        closed = true;
        if (usedBytes != 0) {
            throw new IllegalStateException("off-heap leak: " + usedBytes + " bytes still allocated");
        }
    }

    void onFree(int capacity) {
        long next = usedBytes - capacity;
        if (next < 0) {
            throw new IllegalStateException("allocator accounting underflow");
        }
        usedBytes = next;
    }
}

final class YierdisNettyOffHeapBuf implements YierdisOffHeapBuf {
    private final YierdisNettyOffHeapAllocator owner;
    private final ByteBuf buf;
    private final int capacity;

    private boolean closed;

    YierdisNettyOffHeapBuf(YierdisNettyOffHeapAllocator owner, ByteBuf buf, int capacity) {
        this.owner = owner;
        this.buf = buf;
        this.capacity = capacity;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public byte getByte(int index) {
        ensureOpen();
        return buf.getByte(index);
    }

    @Override
    public void setByte(int index, byte value) {
        ensureOpen();
        buf.setByte(index, value);
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        ensureOpen();
        buf.getBytes(index, dst, dstOff, len);
    }

    @Override
    public void setBytes(int index, byte[] src, int srcOff, int len) {
        ensureOpen();
        buf.setBytes(index, src, srcOff, len);
    }

    @Override
    public YierdisOffHeapSlice slice(int index, int len) {
        ensureOpen();
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (index < 0 || index + len > capacity) {
            throw new IndexOutOfBoundsException();
        }
        return new YierdisNettyOffHeapSlice(this, index, len);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        buf.release();
        owner.onFree(capacity);
    }

    void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("buffer is closed");
        }
    }

    ByteBuf unwrap() {
        return buf;
    }

    int refCnt() {
        return buf.refCnt();
    }
}

final class YierdisNettyOffHeapSlice implements YierdisOffHeapSlice {
    private final YierdisNettyOffHeapBuf owner;
    private final int offset;
    private final int len;

    YierdisNettyOffHeapSlice(YierdisNettyOffHeapBuf owner, int offset, int len) {
        this.owner = owner;
        this.offset = offset;
        this.len = len;
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public byte getByte(int index) {
        owner.ensureOpen();
        if (index < 0 || index >= len) {
            throw new IndexOutOfBoundsException();
        }
        return owner.unwrap().getByte(offset + index);
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int readLen) {
        owner.ensureOpen();
        if (readLen < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (index < 0 || index + readLen > len) {
            throw new IndexOutOfBoundsException();
        }
        owner.unwrap().getBytes(offset + index, dst, dstOff, readLen);
    }

    @Override
    public void writeTo(ByteBuf out) {
        owner.ensureOpen();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        out.writeBytes(owner.unwrap(), offset, len);
    }
}
