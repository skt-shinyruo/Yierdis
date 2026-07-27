package yier.bubu.redis.bytes;

/**
 * A minimal random-access bytes source abstraction.
 */
public interface BytesSource {
    byte getByte(int index);

    void getBytes(int index, byte[] dst, int dstOff, int len);
}
