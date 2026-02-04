package yier.bubu.redis.db.key;

// KeyHandle：key identity SSOT（heap/off-heap/bytesview），用于 keyspace/expires/scan 等内部路径的零拷贝交互。

import yier.bubu.redis.db.YierdisBytesView;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;

import java.util.Objects;

/**
 * Key identity SSOT（Single Source Of Truth）。
 * <p>
 * 设计目标：
 * <ul>
 *   <li>统一 heap/off-heap/bytesview 三种来源的 key 表示</li>
 *   <li>提供 {@code len + byteAt} 的只读访问能力，避免为“canonicalKey”强制产生 heap copy</li>
 *   <li>允许携带“字典 hash”（通常包含 per-dict seed），用于 keyspace 索引；但 equality 以 bytes 内容为准</li>
 * </ul>
 * <p>
 * 生命周期约束：
 * <ul>
 *   <li>该对象可被用于短期查找/迭代；若其底层指向 off-heap 地址，调用方必须确保内存生命周期覆盖使用期</li>
 *   <li>实现必须是只读的，不得暴露可变 byte[] 的写入入口</li>
 * </ul>
 */
public interface KeyHandle extends YierdisBytesView {
    /**
     * 用于 keyspace/hash table 的索引 hash。
     * <p>
     * 该 hash 通常是 “bytes hash ^ seed” 的结果，因此 <b>不保证跨实例/跨后端一致</b>；
     * 但在同一 keyspace 实例内应保持稳定。
     */
    int dictHash();

    static KeyHandle forHeap(byte[] keyBytes, int dictHash) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        return new HeapKeyHandle(keyBytes, dictHash);
    }

    static KeyHandle forOffHeap(YierdisOffHeapAddressAllocator allocator, long address, int len, int dictHash) {
        Objects.requireNonNull(allocator, "allocator");
        return new OffHeapKeyHandle(allocator, address, len, dictHash);
    }
}

