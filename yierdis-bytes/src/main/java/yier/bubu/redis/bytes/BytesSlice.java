package yier.bubu.redis.bytes;

/**
 * A length-bounded slice of bytes that can be written into a {@link BytesSink}.
 * <p>
 * This is used by the server write path to efficiently stream values without forcing heap copies.
 */
public interface BytesSlice extends BytesSource {
    int length();

    void writeTo(BytesSink out);
}
