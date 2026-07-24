package yier.bubu.redis.storage.api;

// ListReadOps：list 只读能力边界。

import yier.bubu.redis.storage.api.result.ByteSequenceSource;

public interface ListReadOps {
    ByteSequenceSource lrange(byte[] keyBytes, int start, int stop);
}
