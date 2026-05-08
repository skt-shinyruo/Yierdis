package yier.bubu.redis.storage.api;

// StringReadOps：string 只读能力边界。

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.result.BulkStringValue;

public interface StringReadOps {
    byte[] getStringBytes(byte[] keyBytes);

    BulkStringValue getStringValue(BytesView keyView);

    long strlen(BytesView keyView);

    int getBit(BytesView keyView, long offset);

    long bitcount(BytesView keyView);

    long bitcount(BytesView keyView, long start, long end);
}
