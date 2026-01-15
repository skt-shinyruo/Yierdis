package yier.bubu.redis.db.offheap.api;

import java.util.Arrays;

public final class YierdisByteArraySink implements YierdisBytesSink {
    private byte[] buf;
    private int size;

    public YierdisByteArraySink() {
        this(64);
    }

    public YierdisByteArraySink(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be >= 0");
        }
        this.buf = new byte[Math.max(0, initialCapacity)];
    }

    public int size() {
        return size;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buf, size);
    }

    @Override
    public void writeBytes(byte[] src, int srcIndex, int len) {
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (srcIndex < 0 || srcIndex + len > src.length) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }

        int nextSize = size + len;
        if (nextSize < 0) {
            throw new IllegalStateException("size overflow");
        }
        ensureCapacity(nextSize);
        System.arraycopy(src, srcIndex, buf, size, len);
        size = nextSize;
    }

    private void ensureCapacity(int desired) {
        if (buf.length >= desired) {
            return;
        }
        int next = Math.max(8, buf.length);
        while (next < desired) {
            int grow = next < 1024 * 1024 ? (next << 1) : (next + 1024 * 1024);
            if (grow <= next) {
                next = desired;
                break;
            }
            next = grow;
        }
        buf = Arrays.copyOf(buf, next);
    }
}

