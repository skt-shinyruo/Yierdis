package yier.bubu.redis.storage.api;

// HashReadOps：hash 只读能力边界。

import yier.bubu.redis.storage.api.result.BulkStringMapMetrics;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public interface HashReadOps {
    BulkStringValue hget(byte[] keyBytes, byte[] fieldBytes);

    BulkStringMapMetrics hgetall(byte[] keyBytes);

    long hlen(byte[] keyBytes);

    CollectionScanWindow hscan(
            byte[] keyBytes,
            ScanCursorV2 cursor,
            byte[] globPattern,
            int count,
            boolean noValues
    );
}
