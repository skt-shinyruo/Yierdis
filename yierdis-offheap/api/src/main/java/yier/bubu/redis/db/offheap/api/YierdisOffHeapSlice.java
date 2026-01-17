package yier.bubu.redis.db.offheap.api;

/**
 * Off-heap 字节切片视图。
 * <p>
 * 该类型仍属于 off-heap API，但它同时实现中立的 {@link yier.bubu.redis.bytes.BytesSlice} 约定，
 * 以便 protocol/server 等模块只依赖中立的 bytes 抽象（SSOT）。
 */
public interface YierdisOffHeapSlice extends yier.bubu.redis.bytes.BytesSlice {
}
