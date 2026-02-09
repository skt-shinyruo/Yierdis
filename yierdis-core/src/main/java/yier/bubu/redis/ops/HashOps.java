package yier.bubu.redis.ops;

// HashOps：hash 类型操作边界（command 层通过该接口访问 hash 行为，避免直接耦合 YierdisDb 单体）。

import yier.bubu.redis.protocol.ReplySink;

import java.util.List;

public interface HashOps {
    long hset(byte[] keyBytes, List<byte[]> fieldValuePairs);

    byte[] hget(byte[] keyBytes, byte[] fieldBytes);

    /**
     * Returns the number of field/value pairs for {@code HGETALL}'s map header.
     */
    int hgetallPairCount(byte[] keyBytes);

    /**
     * Writes field/value pairs into {@code out} in HGETALL order.
     */
    void hgetallWriteTo(byte[] keyBytes, ReplySink out);

    long hlen(byte[] keyBytes);

    long hdel(byte[] keyBytes, List<byte[]> fields);
}
