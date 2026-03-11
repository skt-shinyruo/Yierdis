package yier.bubu.redis.db.offheap.unsafe;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Unsafe access helpers for off-heap backends.
 * <p>
 * 约束：该类是实现细节，不应暴露到上层业务逻辑；它只提供最小必要的 raw memory 操作。
 */
public final class YierdisUnsafeAccess {
    private static final Unsafe UNSAFE = loadUnsafe();
    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private YierdisUnsafeAccess() {
    }

    static Unsafe unsafe() {
        return UNSAFE;
    }

    public static long allocateMemory(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        return UNSAFE.allocateMemory(bytes);
    }

    public static void freeMemory(long address) {
        if (address == 0) {
            throw new IllegalArgumentException("address must be != 0");
        }
        UNSAFE.freeMemory(address);
    }

    public static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    public static void putByte(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    public static long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    public static void putLong(long address, long value) {
        UNSAFE.putLong(address, value);
    }

    public static void setMemory(long address, long bytes, byte value) {
        if (bytes <= 0) {
            return;
        }
        UNSAFE.setMemory(address, bytes, value);
    }

    public static void copyMemory(long srcAddress, long dstAddress, long bytes) {
        if (bytes <= 0) {
            return;
        }
        UNSAFE.copyMemory(null, srcAddress, null, dstAddress, bytes);
    }

    public static void copyMemory(byte[] src, int srcIndex, long dstAddress, int len) {
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len <= 0) {
            return;
        }
        if (srcIndex < 0 || srcIndex + len > src.length) {
            throw new IndexOutOfBoundsException();
        }
        UNSAFE.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + srcIndex, null, dstAddress, len);
    }

    public static void copyMemory(long srcAddress, byte[] dst, int dstIndex, int len) {
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        if (len <= 0) {
            return;
        }
        if (dstIndex < 0 || dstIndex + len > dst.length) {
            throw new IndexOutOfBoundsException();
        }
        UNSAFE.copyMemory(null, srcAddress, dst, BYTE_ARRAY_BASE_OFFSET + dstIndex, len);
    }

    private static Unsafe loadUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}

