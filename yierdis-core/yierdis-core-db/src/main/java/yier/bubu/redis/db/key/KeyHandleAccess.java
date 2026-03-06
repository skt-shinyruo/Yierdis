package yier.bubu.redis.db.key;

// KeyHandleAccess：KeyHandle 的内部访问桥接（用于将 handle 落地到 keyspace/expire index 等实现细节）。

import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;

/**
 * KeyHandle 的内部访问桥接。
 * <p>
 * 目标：
 * - 不把 off-heap address 等细节暴露为 {@link KeyHandle} 公共契约的一部分
 * - 允许内部组件（keyspace/expire index/scan）在已确认 handle 类型的前提下访问实现细节
 */
public final class KeyHandleAccess {
    private KeyHandleAccess() {
    }

    public static boolean isHeap(KeyHandle handle) {
        return handle instanceof HeapKeyHandle;
    }

    public static boolean isOffHeap(KeyHandle handle) {
        return handle instanceof OffHeapKeyHandle;
    }

    public static byte[] heapBytesOrNull(KeyHandle handle) {
        if (handle instanceof HeapKeyHandle h) {
            return h.bytesUnsafe();
        }
        return null;
    }

    public static OffHeapAddressAllocator offHeapAllocator(KeyHandle handle) {
        if (!(handle instanceof OffHeapKeyHandle h)) {
            throw new IllegalArgumentException("not an off-heap KeyHandle: " + (handle == null ? "null" : handle.getClass().getName()));
        }
        return h.allocatorUnsafe();
    }

    public static long offHeapAddress(KeyHandle handle) {
        if (!(handle instanceof OffHeapKeyHandle h)) {
            throw new IllegalArgumentException("not an off-heap KeyHandle: " + (handle == null ? "null" : handle.getClass().getName()));
        }
        return h.addressUnsafe();
    }
}
