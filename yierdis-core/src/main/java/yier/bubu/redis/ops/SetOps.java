package yier.bubu.redis.ops;

// SetOps：set 类型操作边界。

import yier.bubu.redis.ops.result.BulkStringSequence;

import java.util.List;

public interface SetOps {
    long sadd(byte[] keyBytes, List<byte[]> members);

    long srem(byte[] keyBytes, List<byte[]> members);

    BulkStringSequence smembers(byte[] keyBytes);

    boolean sismember(byte[] keyBytes, byte[] member);

    long scard(byte[] keyBytes);
}
