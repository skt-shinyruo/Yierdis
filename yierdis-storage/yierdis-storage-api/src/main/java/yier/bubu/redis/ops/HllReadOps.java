package yier.bubu.redis.ops;

// HllReadOps：HLL 只读能力边界。

import java.util.List;

public interface HllReadOps {
    long pfcount(List<byte[]> keys);
}
