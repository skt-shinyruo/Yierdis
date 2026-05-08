package yier.bubu.redis.storage.api;

// KeyspaceReadOps：keyspace 只读能力边界。

import yier.bubu.redis.bytes.BytesView;

import java.util.List;

public interface KeyspaceReadOps {
    ValueType typeOf(BytesView keyView);

    boolean existsKey(BytesView keyView);

    List<byte[]> keys(byte[] globPattern, int maxMatches, long timeBudgetNanos);

    ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out);
}
