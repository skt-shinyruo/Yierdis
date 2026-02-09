package yier.bubu.redis.ops;

// MemoryOps：内存/对象诊断能力边界（MEMORY/OBJECT 等）。

import yier.bubu.redis.db.YierdisBytesView;
import yier.bubu.redis.db.YierdisMemoryStats;

public interface MemoryOps {
    long memoryUsage(YierdisBytesView keyView);

    YierdisMemoryStats memoryStats();

    String objectEncoding(YierdisBytesView keyView);
}

