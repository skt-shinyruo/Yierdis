package yier.bubu.redis.storage.api;

// HashReadOps：hash 只读能力边界。

import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public interface HashReadOps {
    ByteValue hget(byte[] keyBytes, byte[] fieldBytes);

    ByteMapSource hgetall(byte[] keyBytes);

    long hlen(byte[] keyBytes);

    CollectionScanWindow hscan(
            byte[] keyBytes,
            ScanCursorV2 cursor,
            byte[] globPattern,
            int count,
            boolean noValues
    );
}
