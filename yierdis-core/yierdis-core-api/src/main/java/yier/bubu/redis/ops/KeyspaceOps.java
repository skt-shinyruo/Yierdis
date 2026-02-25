package yier.bubu.redis.ops;

// KeyspaceOps：keyspace 能力边界（TYPE/DEL/EXISTS/KEYS/SCAN 等）。

import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.bytes.BytesView;

import java.util.Collection;
import java.util.List;

public interface KeyspaceOps {
    ValueType typeOf(BytesView keyView);

    long del(Collection<byte[]> keys);

    boolean existsKey(BytesView keyView);

    List<byte[]> keys(byte[] globPattern, int maxMatches, long timeBudgetNanos);

    ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out);
}

