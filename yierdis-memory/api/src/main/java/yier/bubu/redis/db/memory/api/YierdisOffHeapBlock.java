package yier.bubu.redis.db.memory.api;

/**
 * A raw off-heap memory block backed by an address with deterministic free via {@link #close()}.
 * <p>
 * This is an optional capability used by some backends to implement off-heap index/data structures.
 */
public interface YierdisOffHeapBlock extends yier.bubu.redis.offheap.api.OffHeapBlock {
}
