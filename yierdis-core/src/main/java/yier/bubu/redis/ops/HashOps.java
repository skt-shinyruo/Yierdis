package yier.bubu.redis.ops;

// HashOps：hash 类型操作边界（command 层通过该接口访问 hash 行为，避免直接耦合 YierdisDb 单体）。

import yier.bubu.redis.db.YierdisBulkStringOutput;

import java.util.List;

public interface HashOps {
    long hset(byte[] keyBytes, List<byte[]> fieldValuePairs);

    byte[] hget(byte[] keyBytes, byte[] fieldBytes);

    int hgetallReplyCount(byte[] keyBytes);

    void hgetallReplyInto(byte[] keyBytes, YierdisBulkStringOutput out);

    long hlen(byte[] keyBytes);

    long hdel(byte[] keyBytes, List<byte[]> fields);
}

