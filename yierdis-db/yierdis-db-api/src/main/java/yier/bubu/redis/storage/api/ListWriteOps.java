package yier.bubu.redis.storage.api;

// ListWriteOps：list 写能力边界。

import java.util.List;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

public interface ListWriteOps {
    WriteResult<Long> lpush(byte[] keyBytes, List<byte[]> values);

    WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values);

    WriteResult<PoppedValueSequence> lpop(byte[] keyBytes, int count);

    WriteResult<PoppedValueSequence> rpop(byte[] keyBytes, int count);
}
