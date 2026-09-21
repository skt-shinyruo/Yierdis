package yier.bubu.redis.bytes;

/**
 * 带长度的随机访问 bytes 只读视图（Netty-free）。
 * <p>
 * 主要用于 key 等请求级 lookup 的输入视图；实现必须视为短生命周期对象，不得被存入 DB。
 */
public interface BytesView {
    int length();

    byte getByte(int index);

    default void getBytes(int index, byte[] dst, int dstOff, int len) {
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (len == 0) {
            return;
        }
        if (index < 0 || dstOff < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (dstOff + len > dst.length) {
            throw new IndexOutOfBoundsException();
        }
        for (int i = 0; i < len; i++) {
            dst[dstOff + i] = getByte(index + i);
        }
    }
}
