package yier.bubu.redis.ops.result;

// BulkStringMapPairs：用于表达 map 的 pairs（field/value）序列，命令层写 mapHeader(pairCount) 后再 emitPairsTo。

public interface BulkStringMapPairs {
    /**
     * 返回 key/value 键值对数量。
     */
    int pairCount();

    /**
     * 以 key0, value0, key1, value1, ... 的顺序同步输出所有键值对。
     */
    void emitPairsTo(BulkStringSink out);
}
