package yier.bubu.redis.memory.api;

/**
 * 在受限生命周期内访问稳定对象的当前内容。
 * 视图沿用后端 owner 和访问模式；只读视图拒绝写入，关闭后任何内容访问都无效。
 * 普通 resolve 的视图释放自身保留，resolvePinned 的视图不会替调用方 unpin。
 */
public interface NativeObjectView extends AutoCloseable {
    NativeHandle handle();

    int size();

    int capacity();

    byte getByte(int index);

    void setByte(int index, byte value);

    void getBytes(int index, byte[] dst, int dstOff, int len);

    void setBytes(int index, byte[] src, int srcOff, int len);

    default void copyBytes(int sourceIndex, int targetIndex, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        if (sourceIndex < 0 || targetIndex < 0
                || sourceIndex > size() - length || targetIndex > size() - length) {
            throw new IndexOutOfBoundsException();
        }
        if (length == 0 || sourceIndex == targetIndex) {
            return;
        }
        if (targetIndex < sourceIndex) {
            for (int offset = 0; offset < length; offset++) {
                setByte(targetIndex + offset, getByte(sourceIndex + offset));
            }
            return;
        }
        for (int offset = length - 1; offset >= 0; offset--) {
            setByte(targetIndex + offset, getByte(sourceIndex + offset));
        }
    }

    default boolean contentEquals(int index, byte[] other, int otherOffset, int length) {
        if (other == null) {
            throw new IllegalArgumentException("other must not be null");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        if (index < 0 || otherOffset < 0
                || index > size() - length || otherOffset > other.length - length) {
            throw new IndexOutOfBoundsException();
        }
        for (int offset = 0; offset < length; offset++) {
            if (getByte(index + offset) != other[otherOffset + offset]) {
                return false;
            }
        }
        return true;
    }

    default int getIntLittleEndian(int index) {
        return (getByte(index) & 0xff)
                | ((getByte(index + 1) & 0xff) << 8)
                | ((getByte(index + 2) & 0xff) << 16)
                | ((getByte(index + 3) & 0xff) << 24);
    }

    default void setIntLittleEndian(int index, int value) {
        for (int offset = 0; offset < Integer.BYTES; offset++) {
            setByte(index + offset, (byte) (value >>> (offset * 8)));
        }
    }

    default long getLongLittleEndian(int index) {
        return ((long) getByte(index) & 0xff)
                | (((long) getByte(index + 1) & 0xff) << 8)
                | (((long) getByte(index + 2) & 0xff) << 16)
                | (((long) getByte(index + 3) & 0xff) << 24)
                | (((long) getByte(index + 4) & 0xff) << 32)
                | (((long) getByte(index + 5) & 0xff) << 40)
                | (((long) getByte(index + 6) & 0xff) << 48)
                | (((long) getByte(index + 7) & 0xff) << 56);
    }

    default void setLongLittleEndian(int index, long value) {
        for (int offset = 0; offset < Long.BYTES; offset++) {
            setByte(index + offset, (byte) (value >>> (offset * 8)));
        }
    }

    @Override
    void close();
}
