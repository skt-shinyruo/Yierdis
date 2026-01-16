package yier.bubu.redis.db.offheap;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;

/**
 * Helpers for SDS-like raw off-heap strings stored as a single allocation:
 * {@code [int len][int cap][byte data...]}.
 * <p>
 * This is intentionally low-level and uses raw addresses as handles.
 */
public final class YierdisUnsafeOffHeapSds {
    private static final int HEADER_BYTES = Integer.BYTES + Integer.BYTES;

    private YierdisUnsafeOffHeapSds() {
    }

    public static long allocate(YierdisOffHeapAddressAllocator allocator, byte[] src, int off, int len) {
        if (allocator == null) {
            throw new IllegalArgumentException("allocator must not be null");
        }
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (off < 0 || off + len > src.length) {
            throw new IndexOutOfBoundsException();
        }

        int cap = len;
        long base = allocator.allocateAddress(Math.max(8, totalBytes(cap)));
        writeInt(allocator, base, len);
        writeInt(allocator, base + Integer.BYTES, cap);
        if (len > 0) {
            allocator.copyMemory(src, off, base + HEADER_BYTES, len);
        }
        return base;
    }

    public static void free(YierdisOffHeapAddressAllocator allocator, long baseAddr) {
        if (allocator == null) {
            throw new IllegalArgumentException("allocator must not be null");
        }
        if (baseAddr == 0) {
            return;
        }
        int cap = readInt(allocator, baseAddr + Integer.BYTES);
        allocator.freeAddress(baseAddr, Math.max(8, totalBytes(cap)));
    }

    public static int len(YierdisOffHeapAddressAllocator allocator, long baseAddr) {
        if (allocator == null) {
            throw new IllegalArgumentException("allocator must not be null");
        }
        return readInt(allocator, baseAddr);
    }

    public static long dataAddress(long baseAddr) {
        return baseAddr + HEADER_BYTES;
    }

    public static YierdisOffHeapSlice slice(YierdisOffHeapAddressAllocator allocator, long baseAddr) {
        int len = len(allocator, baseAddr);
        if (len == 0) {
            return new YierdisUnsafeOffHeapRawSlice(allocator, baseAddr + HEADER_BYTES, 0);
        }
        return new YierdisUnsafeOffHeapRawSlice(allocator, baseAddr + HEADER_BYTES, len);
    }

    public static void getBytes(YierdisOffHeapAddressAllocator allocator, long baseAddr, byte[] dst, int dstOff, int len) {
        if (allocator == null) {
            throw new IllegalArgumentException("allocator must not be null");
        }
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (dstOff < 0 || dstOff + len > dst.length) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        allocator.copyMemory(baseAddr + HEADER_BYTES, dst, dstOff, len);
    }

    private static int totalBytes(int cap) {
        return HEADER_BYTES + Math.max(0, cap);
    }

    private static int readInt(YierdisOffHeapAddressAllocator allocator, long addr) {
        int b0 = allocator.getByte(addr) & 0xff;
        int b1 = allocator.getByte(addr + 1) & 0xff;
        int b2 = allocator.getByte(addr + 2) & 0xff;
        int b3 = allocator.getByte(addr + 3) & 0xff;
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    }

    private static void writeInt(YierdisOffHeapAddressAllocator allocator, long addr, int value) {
        allocator.putByte(addr, (byte) value);
        allocator.putByte(addr + 1, (byte) (value >>> 8));
        allocator.putByte(addr + 2, (byte) (value >>> 16));
        allocator.putByte(addr + 3, (byte) (value >>> 24));
    }
}
