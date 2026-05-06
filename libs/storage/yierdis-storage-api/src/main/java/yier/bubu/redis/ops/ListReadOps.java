package yier.bubu.redis.ops;

// ListReadOps：list 只读能力边界。

import yier.bubu.redis.ops.result.BulkStringSequence;

public interface ListReadOps {
    BulkStringSequence lrange(byte[] keyBytes, int start, int stop);
}
