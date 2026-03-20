package yier.bubu.redis.ops;

// HashWriteOps：hash 写能力边界。

import java.util.List;

public interface HashWriteOps {
    long hset(byte[] keyBytes, List<byte[]> fieldValuePairs);

    long hdel(byte[] keyBytes, List<byte[]> fields);
}
