package yier.bubu.redis.memory.api;

/**
 * 由后端分配并计入其内存用量的固定大小区域。
 * 多字节整数固定使用 little-endian，{@link #copyTo} 在源和目标重叠时也保持复制语义。
 * 复制只传递字节，不转移任一区域的所有权。区域沿用后端 owner 约束；
 * 调用方须在关闭后端前关闭区域，关闭后不得继续访问。
 */
public interface StableMemoryRegion extends AutoCloseable {
    int size();
    byte getByte(int offset);
    void setByte(int offset, byte value);
    int getIntLittleEndian(int offset);
    void setIntLittleEndian(int offset, int value);
    long getLongLittleEndian(int offset);
    void setLongLittleEndian(int offset, long value);
    void getBytes(int offset, byte[] dst, int dstOffset, int length);
    void setBytes(int offset, byte[] src, int srcOffset, int length);
    void copyTo(int sourceOffset, StableMemoryRegion target, int targetOffset, int length);

    /** 释放区域；重复关闭不重复释放资源或扣减后端统计。 */
    @Override
    void close();
}
