package yier.bubu.redis.memory.api;

public interface NativeObjectView extends AutoCloseable {
    NativeHandle handle();

    int size();

    int capacity();

    byte getByte(int index);

    void setByte(int index, byte value);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void setBytes(int index, byte[] src, int srcOff, int len);

    @Override
    void close();
}
