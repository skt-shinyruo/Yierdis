package yier.bubu.redis.db.offheap.api;

/**
 * Off-heap view of a byte slice.
 * <p>
 * This type is still part of the off-heap API, but it also implements the neutral {@link yier.bubu.redis.bytes.BytesSlice}
 * contract so server protocol code can depend on the neutral module.
 */
public interface YierdisOffHeapSlice extends yier.bubu.redis.bytes.BytesSlice {
    @Override
    default void writeTo(yier.bubu.redis.bytes.BytesSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (out instanceof YierdisBytesSink sink) {
            writeTo(sink);
            return;
        }
        // Fallback: copy into a small heap buffer.
        int len = length();
        if (len <= 0) {
            return;
        }
        byte[] buf = new byte[Math.min(len, 8192)];
        int p = 0;
        while (p < len) {
            int n = Math.min(buf.length, len - p);
            getBytes(p, buf, 0, n);
            out.writeBytes(buf, 0, n);
            p += n;
        }
    }

    void writeTo(YierdisBytesSink out);
}
