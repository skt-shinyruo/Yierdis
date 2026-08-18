package yier.bubu.redis.storage.api;

import java.util.Collection;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.result.KeyScanWindow;

public interface KeyspaceOps {
    ValueType typeOf(BytesView keyView);

    boolean existsKey(BytesView keyView);

    KeyScanWindow keys(byte[] globPattern, int maxMatches, long timeBudgetNanos);

    KeyScanWindow scan(ScanCursorV2 cursor, byte[] globPattern, int count);

    WriteResult<Long> del(Collection<byte[]> keys);
}
