package yier.bubu.redis.db.offheap.api;

/**
 * Optional capability interface for off-heap allocators that expose raw address-based memory operations.
 * <p>
 * This is primarily used by internal off-heap index/data structures (e.g. hash tables, slot arrays).
 * <p>
 * Implementations must provide a consistent view over the returned addresses for the lifetime of the blocks,
 * and {@link #close()} MUST free the underlying memory exactly once.
 */
public interface YierdisOffHeapAddressAllocator
        extends yier.bubu.redis.offheap.api.OffHeapAddressAllocator, YierdisOffHeapAllocator {
    /**
     * Allocates a raw block and returns an owning handle that will free memory on {@link YierdisOffHeapBlock#close()}.
     */
    YierdisOffHeapBlock allocateBlock(int capacity);

    long allocateAddress(int capacity);

    void freeAddress(long address, int capacity);

    byte getByte(long address);

    void putByte(long address, byte value);

    void setMemory(long address, long bytes, byte value);

    void copyMemory(long srcAddress, long dstAddress, long bytes);

    void copyMemory(byte[] src, int srcIndex, long dstAddress, int len);

    void copyMemory(long srcAddress, byte[] dst, int dstIndex, int len);
}
