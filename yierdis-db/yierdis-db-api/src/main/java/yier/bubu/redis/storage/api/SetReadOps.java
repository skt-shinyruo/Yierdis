package yier.bubu.redis.storage.api;

// SetReadOps：set 只读能力边界。

import yier.bubu.redis.storage.api.result.BulkStringSequence;

public interface SetReadOps {
    BulkStringSequence smembers(byte[] keyBytes);

    boolean sismember(byte[] keyBytes, byte[] member);

    long scard(byte[] keyBytes);
}
