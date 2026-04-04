package yier.bubu.redis.db.key;

// KeyHandleAccess：KeyHandle 的内部访问桥接（用于将 handle 落地到 keyspace/expire index 等实现细节）。

import yier.bubu.redis.db.memory.ffm.YierdisFfmBytesRef;

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

    public static boolean isFfm(KeyHandle handle) {
        return handle instanceof FfmKeyHandle;
    }

    public static byte[] heapBytesOrNull(KeyHandle handle) {
        if (handle instanceof HeapKeyHandle h) {
            return h.bytesUnsafe();
        }
        return null;
    }

    public static YierdisFfmBytesRef ffmBytesRef(KeyHandle handle) {
        if (!(handle instanceof FfmKeyHandle h)) {
            throw new IllegalArgumentException("not an FFM KeyHandle: " + (handle == null ? "null" : handle.getClass().getName()));
        }
        return h.refUnsafe();
    }

    public static YierdisFfmBytesRef ffmBytesRefOrNull(KeyHandle handle) {
        if (handle instanceof FfmKeyHandle h) {
            return h.refUnsafe();
        }
        return null;
    }

}
