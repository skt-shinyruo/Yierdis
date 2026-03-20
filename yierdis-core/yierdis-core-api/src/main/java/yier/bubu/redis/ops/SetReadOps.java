package yier.bubu.redis.ops;

// SetReadOps：set 只读能力边界。

import yier.bubu.redis.ops.result.BulkStringSequence;

public interface SetReadOps {
    BulkStringSequence smembers(byte[] keyBytes);

    boolean sismember(byte[] keyBytes, byte[] member);

    long scard(byte[] keyBytes);
}
