package yier.bubu.redis.memory.api;

import yier.bubu.redis.bytes.BytesSource;

public interface OffHeapBuf extends AutoCloseable {
    int capacity();

    byte getByte(int index);

    void setByte(int index, byte value);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void setBytes(int index, byte[] src, int srcOff, int len);

    void setBytes(int index, BytesSource src, int srcIndex, int len);

    OffHeapSlice slice(int index, int len);

    @Override
    void close();
}

