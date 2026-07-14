package yier.bubu.redis.storage.api;

// KeyspaceReadOps：keyspace 只读能力边界。

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.result.KeyScanWindow;

public interface KeyspaceReadOps {
    ValueType typeOf(BytesView keyView);

    boolean existsKey(BytesView keyView);

    KeyScanWindow keys(byte[] globPattern, int maxMatches, long timeBudgetNanos);

    KeyScanWindow scan(ScanCursorV2 cursor, byte[] globPattern, int count);
}
