package yier.bubu.redis.bytes;

/**
 * A minimal bytes sink abstraction.
 * <p>
 * This package is intentionally neutral: it is shared by protocol, off-heap and I/O adapters.
 */
public interface BytesSink {
    void writeBytes(byte[] src, int srcIndex, int len);
}
