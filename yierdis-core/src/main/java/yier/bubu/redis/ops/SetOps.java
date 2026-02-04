package yier.bubu.redis.ops;

// SetOps：set 类型操作边界。

import yier.bubu.redis.db.YierdisBulkStringOutput;

import java.util.List;

public interface SetOps {
    long sadd(byte[] keyBytes, List<byte[]> members);

    long srem(byte[] keyBytes, List<byte[]> members);

    int smembersReplyCount(byte[] keyBytes);

    void smembersReplyInto(byte[] keyBytes, YierdisBulkStringOutput out);

    boolean sismember(byte[] keyBytes, byte[] member);

    long scard(byte[] keyBytes);
}

