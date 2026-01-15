package yier.bubu.redis.db.offheap;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeAccess;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;

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

    public static long allocate(YierdisUnsafeOffHeapAllocator allocator, byte[] src, int off, int len) {
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
        writeInt(base, len);
        writeInt(base + Integer.BYTES, cap);
        if (len > 0) {
            YierdisUnsafeAccess.copyMemory(src, off, base + HEADER_BYTES, len);
        }
        return base;
    }

    public static void free(YierdisUnsafeOffHeapAllocator allocator, long baseAddr) {
        if (allocator == null) {
            throw new IllegalArgumentException("allocator must not be null");
        }
        if (baseAddr == 0) {
            return;
        }
        int cap = readInt(baseAddr + Integer.BYTES);
        allocator.freeAddress(baseAddr, Math.max(8, totalBytes(cap)));
    }

    public static int len(long baseAddr) {
        return readInt(baseAddr);
    }

    public static long dataAddress(long baseAddr) {
        return baseAddr + HEADER_BYTES;
    }

    public static YierdisOffHeapSlice slice(long baseAddr) {
        int len = len(baseAddr);
        if (len == 0) {
            return new YierdisUnsafeOffHeapRawSlice(baseAddr + HEADER_BYTES, 0);
        }
        return new YierdisUnsafeOffHeapRawSlice(baseAddr + HEADER_BYTES, len);
    }

    public static void getBytes(long baseAddr, byte[] dst, int dstOff, int len) {
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
        YierdisUnsafeAccess.copyMemory(baseAddr + HEADER_BYTES, dst, dstOff, len);
    }

    private static int totalBytes(int cap) {
        return HEADER_BYTES + Math.max(0, cap);
    }

    private static int readInt(long addr) {
        int b0 = YierdisUnsafeAccess.getByte(addr) & 0xff;
        int b1 = YierdisUnsafeAccess.getByte(addr + 1) & 0xff;
        int b2 = YierdisUnsafeAccess.getByte(addr + 2) & 0xff;
        int b3 = YierdisUnsafeAccess.getByte(addr + 3) & 0xff;
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    }

    private static void writeInt(long addr, int value) {
        YierdisUnsafeAccess.putByte(addr, (byte) value);
        YierdisUnsafeAccess.putByte(addr + 1, (byte) (value >>> 8));
        YierdisUnsafeAccess.putByte(addr + 2, (byte) (value >>> 16));
        YierdisUnsafeAccess.putByte(addr + 3, (byte) (value >>> 24));
    }
}
