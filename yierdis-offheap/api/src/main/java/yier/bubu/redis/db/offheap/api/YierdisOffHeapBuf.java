package yier.bubu.redis.db.offheap.api;

import io.netty.buffer.ByteBuf;

public interface YierdisOffHeapBuf extends AutoCloseable {
    int capacity();

    byte getByte(int index);

    void setByte(int index, byte value);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void setBytes(int index, byte[] src, int srcOff, int len);

    void setBytes(int index, ByteBuf src, int srcIndex, int len);

    YierdisOffHeapSlice slice(int index, int len);

    @Override
    void close();
}
