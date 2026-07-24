package yier.bubu.redis.storage.api;

// SetReadOps：set 只读能力边界。

import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;

public interface SetReadOps {
    ByteSequenceSource smembers(byte[] keyBytes);

    boolean sismember(byte[] keyBytes, byte[] member);

    long scard(byte[] keyBytes);

    CollectionScanWindow sscan(byte[] keyBytes, ScanCursorV2 cursor, byte[] globPattern, int count);
}
