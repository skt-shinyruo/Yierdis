package yier.bubu.redis.ops;

// HllOps：HLL（PFADD/PFCOUNT）边界。

import java.util.List;

public interface HllOps {
    int pfadd(byte[] keyBytes, List<byte[]> elements);

    long pfcount(List<byte[]> keys);

    void pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys);
}
