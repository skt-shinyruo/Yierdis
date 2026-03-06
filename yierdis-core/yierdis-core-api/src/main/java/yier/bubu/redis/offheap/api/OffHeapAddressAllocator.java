package yier.bubu.redis.offheap.api;

/**
 * Optional capability interface for off-heap allocators that expose raw address-based memory operations.
 */
public interface OffHeapAddressAllocator extends OffHeapAllocator {
    OffHeapBlock allocateBlock(int capacity);

    long allocateAddress(int capacity);

    void freeAddress(long address, int capacity);

    byte getByte(long address);

    void putByte(long address, byte value);

    void setMemory(long address, long bytes, byte value);

    void copyMemory(long srcAddress, long dstAddress, long bytes);

    void copyMemory(byte[] src, int srcIndex, long dstAddress, int len);

    void copyMemory(long srcAddress, byte[] dst, int dstIndex, int len);
}

