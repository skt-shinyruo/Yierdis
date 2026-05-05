package yier.bubu.redis.ops;

// HashWriteOps：hash 写能力边界。

import java.util.List;

public interface HashWriteOps {
    WriteResult<Long> hset(byte[] keyBytes, List<byte[]> fieldValuePairs);

    WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields);
}
