package yier.bubu.redis.ops;

// KeyspaceOps：keyspace 能力边界（TYPE/DEL/EXISTS/KEYS/SCAN 等）。

import yier.bubu.redis.db.ScanCursorV2;
import yier.bubu.redis.db.ValueType;
import yier.bubu.redis.db.YierdisBytesView;

import java.util.Collection;
import java.util.List;

public interface KeyspaceOps {
    ValueType typeOf(YierdisBytesView keyView);

    long del(Collection<byte[]> keys);

    boolean existsKey(YierdisBytesView keyView);

    List<byte[]> keys(byte[] globPattern, int maxMatches, long timeBudgetNanos);

    ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out);
}

