package yier.bubu.redis.storage.api;

import java.util.List;

public interface HllOps {
    long pfcount(List<byte[]> keys);

    WriteResult<Integer> pfadd(byte[] keyBytes, List<byte[]> elements);

    WriteResult<Void> pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys);
}
