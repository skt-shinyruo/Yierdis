package yier.bubu.redis.storage.api;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.result.KeyScanWindow;

public interface KeyspaceOps {
    ValueType typeOf(BytesView keyView);

    boolean existsKey(BytesView keyView);

    KeyScanWindow keys(byte[] globPattern, int maxMatches, long timeBudgetNanos);

    default KeyScanWindow scan(ScanCursorV2 cursor, byte[] globPattern, int count) {
        return scan(cursor, globPattern, EnumSet.allOf(ValueType.class), count);
    }

    /**
     * 只发现类型属于 {@code types} 的 key。空集合不匹配任何 key，但 cursor 仍按 {@code count}
     * 对应的工作量推进，最终回到 0。
     */
    KeyScanWindow scan(ScanCursorV2 cursor, byte[] globPattern, Set<ValueType> types, int count);

    WriteResult<Long> del(Collection<byte[]> keys);
}
