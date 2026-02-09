package yier.bubu.redis.ops;

// SetOps：set 类型操作边界。

import yier.bubu.redis.protocol.ReplySink;

import java.util.List;

public interface SetOps {
    long sadd(byte[] keyBytes, List<byte[]> members);

    long srem(byte[] keyBytes, List<byte[]> members);

    int smembersCount(byte[] keyBytes);

    void smembersWriteTo(byte[] keyBytes, ReplySink out);

    boolean sismember(byte[] keyBytes, byte[] member);

    long scard(byte[] keyBytes);
}
