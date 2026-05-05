package yier.bubu.redis.ops;

// SetWriteOps：set 写能力边界。

import java.util.List;

public interface SetWriteOps {
    WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members);

    WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members);
}
