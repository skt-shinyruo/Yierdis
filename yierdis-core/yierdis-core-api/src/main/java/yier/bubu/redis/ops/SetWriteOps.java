package yier.bubu.redis.ops;

// SetWriteOps：set 写能力边界。

import java.util.List;

public interface SetWriteOps {
    long sadd(byte[] keyBytes, List<byte[]> members);

    long srem(byte[] keyBytes, List<byte[]> members);
}
