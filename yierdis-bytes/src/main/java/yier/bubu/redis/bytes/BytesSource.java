package yier.bubu.redis.bytes;

/**
 * A minimal random-access bytes source abstraction.
 * <p>
 * Implementations may optionally expose a stable memory address for performance optimizations.
 */
public interface BytesSource {
    byte getByte(int index);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    default boolean hasMemoryAddress() {
        return false;
    }

    default long memoryAddress() {
        throw new UnsupportedOperationException("memoryAddress not supported");
    }
}
