package yier.bubu.redis.ops;

// HllWriteOps：HLL 写能力边界。

import java.util.List;

public interface HllWriteOps {
    int pfadd(byte[] keyBytes, List<byte[]> elements);

    void pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys);
}
