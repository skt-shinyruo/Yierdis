package yier.bubu.redis.storage.api;

// ListReadOps：list 只读能力边界。

import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequence;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

public interface ListReadOps {
    MeasuredBulkStringSequence lrange(byte[] keyBytes, int start, int stop);

    PoppedValueSequence previewPop(byte[] keyBytes, int count, boolean left);
}
