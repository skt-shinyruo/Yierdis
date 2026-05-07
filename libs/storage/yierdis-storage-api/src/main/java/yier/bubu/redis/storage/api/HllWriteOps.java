package yier.bubu.redis.storage.api;

// HllWriteOps：HLL 写能力边界。

import java.util.List;

public interface HllWriteOps {
    WriteResult<Integer> pfadd(byte[] keyBytes, List<byte[]> elements);

    WriteResult<Void> pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys);
}
