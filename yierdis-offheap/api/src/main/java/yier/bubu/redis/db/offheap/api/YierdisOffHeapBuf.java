package yier.bubu.redis.db.offheap.api;

public interface YierdisOffHeapBuf extends AutoCloseable {
    int capacity();

    byte getByte(int index);

    void setByte(int index, byte value);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void setBytes(int index, byte[] src, int srcOff, int len);

    YierdisOffHeapSlice slice(int index, int len);

    @Override
    void close();
}
