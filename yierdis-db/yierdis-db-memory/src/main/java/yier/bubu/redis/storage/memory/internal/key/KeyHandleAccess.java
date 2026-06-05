package yier.bubu.redis.storage.memory.internal.key;

import yier.bubu.redis.memory.api.NativeHandle;

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

    public static boolean isAllocator(KeyHandle handle) {
        return handle instanceof AllocatorKeyHandle;
    }

    public static NativeHandle allocatorNativeHandle(KeyHandle handle) {
        NativeHandle nativeHandle = allocatorNativeHandleOrNull(handle);
        if (nativeHandle == null) {
            throw new IllegalArgumentException("expected allocator-backed KeyHandle: "
                    + (handle == null ? "null" : handle.getClass().getName()));
        }
        return nativeHandle;
    }

    public static NativeHandle allocatorNativeHandleOrNull(KeyHandle handle) {
        if (handle instanceof AllocatorKeyHandle h) {
            return h.nativeHandle();
        }
        return null;
    }
}
