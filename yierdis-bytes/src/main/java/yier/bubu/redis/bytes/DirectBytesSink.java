package yier.bubu.redis.bytes;

/**
 * A sink that supports direct (possibly off-heap) writes and exposes writer cursor state.
 */
public interface DirectBytesSink extends BytesSink {
    void ensureWritable(int len);

    int writerIndex();

    void writerIndex(int writerIndex);

    boolean hasMemoryAddress();

    long memoryAddress();
}
