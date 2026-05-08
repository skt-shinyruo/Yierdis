package yier.bubu.redis.storage.api;

// ListReadOps：list 只读能力边界。

import yier.bubu.redis.storage.api.result.BulkStringSequence;

public interface ListReadOps {
    BulkStringSequence lrange(byte[] keyBytes, int start, int stop);
}
