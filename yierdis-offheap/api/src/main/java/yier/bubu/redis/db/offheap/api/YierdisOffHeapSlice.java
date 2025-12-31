package yier.bubu.redis.db.offheap.api;

import io.netty.buffer.ByteBuf;

public interface YierdisOffHeapSlice {
    int length();

    byte getByte(int index);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void writeTo(ByteBuf out);
}
