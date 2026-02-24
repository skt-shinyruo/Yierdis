package yier.bubu.redis.ops;

// ListOps：list 类型操作边界（command 层通过该接口访问 list 行为）。

import yier.bubu.redis.ops.result.BulkStringSequence;

import java.util.List;

public interface ListOps {
    long lpush(byte[] keyBytes, List<byte[]> values);

    long rpush(byte[] keyBytes, List<byte[]> values);

    BulkStringSequence lrange(byte[] keyBytes, int start, int stop);

    List<byte[]> lpop(byte[] keyBytes, int count);

    List<byte[]> rpop(byte[] keyBytes, int count);
}
