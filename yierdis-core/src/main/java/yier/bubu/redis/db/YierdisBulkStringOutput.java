package yier.bubu.redis.db;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;

/**
 * A minimal bridge for streaming bulk string replies using slice/len semantics.
 * <p>
 * Implementations are expected to write complete RESP2 bulk string frames.
 */
public interface YierdisBulkStringOutput {
    void bulkString(byte[] buf, int off, int len);

    void bulkString(YierdisOffHeapSlice slice);

    void bulkStringNull();

    void bulkStringLongAscii(long value);
}
