package yier.bubu.redis.db.offheap.api;

public interface YierdisOffHeapSlice {
    int length();

    byte getByte(int index);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void writeTo(YierdisBytesSink out);
}
