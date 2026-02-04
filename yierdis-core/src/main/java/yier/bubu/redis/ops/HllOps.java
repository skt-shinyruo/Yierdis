package yier.bubu.redis.ops;

// HllOps：HLL（PFADD/PFCOUNT）边界。

import yier.bubu.redis.protocol.RespCommand;

import java.util.List;

public interface HllOps {
    int pfadd(byte[] keyBytes, RespCommand cmd, int firstElementArgIndex);

    long pfcount(List<byte[]> keys);

    void pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys);
}
