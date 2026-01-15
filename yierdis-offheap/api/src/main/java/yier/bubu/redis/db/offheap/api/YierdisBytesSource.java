package yier.bubu.redis.db.offheap.api;

public interface YierdisBytesSource {
    byte getByte(int index);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    default boolean hasMemoryAddress() {
        return false;
    }

    default long memoryAddress() {
        throw new UnsupportedOperationException("memoryAddress not supported");
    }
}

