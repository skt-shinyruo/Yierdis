package yier.bubu.redis.db.offheap.api;

public interface YierdisBytesSink {
    void writeBytes(byte[] src, int srcIndex, int len);

    default void writeBytes(byte[] src) {
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        writeBytes(src, 0, src.length);
    }
}

