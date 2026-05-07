package yier.bubu.redis.storage.api;

// HashReadOps：hash 只读能力边界。

import yier.bubu.redis.storage.api.result.BulkStringMapPairs;

public interface HashReadOps {
    byte[] hget(byte[] keyBytes, byte[] fieldBytes);

    BulkStringMapPairs hgetall(byte[] keyBytes);

    long hlen(byte[] keyBytes);
}
