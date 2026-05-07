package yier.bubu.redis.storage.api;

// ListWriteOps：list 写能力边界。

import java.util.List;

public interface ListWriteOps {
    WriteResult<Long> lpush(byte[] keyBytes, List<byte[]> values);

    WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values);

    WriteResult<List<byte[]>> lpop(byte[] keyBytes, int count);

    WriteResult<List<byte[]>> rpop(byte[] keyBytes, int count);
}
