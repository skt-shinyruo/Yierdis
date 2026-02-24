package yier.bubu.redis.ops;

// MemoryOps：内存/对象诊断能力边界（MEMORY/OBJECT 等）。

import yier.bubu.redis.bytes.BytesView;

public interface MemoryOps {
    long memoryUsage(BytesView keyView);

    YierdisMemoryStats memoryStats();

    String objectEncoding(BytesView keyView);
}
