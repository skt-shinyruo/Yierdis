package yier.bubu.redis.db.offheap.unsafe;

import io.netty.util.internal.PlatformDependent;

/**
 * A single façade for Netty internal {@link PlatformDependent} usages inside the unsafe backend.\n
 * <p>
 * Keeping all direct Netty-internal calls here makes auditing and future replacement (e.g. pure Unsafe / Foreign)
 * easier.
 */
final class NettyPlatformDependentMemoryAccess {
    private NettyPlatformDependentMemoryAccess() {
    }

    static long allocateMemory(long bytes) {
        return PlatformDependent.allocateMemory(bytes);
    }

    static void freeMemory(long address) {
        PlatformDependent.freeMemory(address);
    }

    static byte getByte(long address) {
        return PlatformDependent.getByte(address);
    }

    static void putByte(long address, byte value) {
        PlatformDependent.putByte(address, value);
    }

    static void copyMemory(long srcAddress, long dstAddress, long bytes) {
        PlatformDependent.copyMemory(srcAddress, dstAddress, bytes);
    }

    static void copyMemory(byte[] src, int srcIndex, long dstAddress, int len) {
        PlatformDependent.copyMemory(src, srcIndex, dstAddress, len);
    }

    static void copyMemory(long srcAddress, byte[] dst, int dstIndex, int len) {
        PlatformDependent.copyMemory(srcAddress, dst, dstIndex, len);
    }
}

