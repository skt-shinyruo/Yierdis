package yier.bubu.redis.storage.api;

import java.util.List;
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public interface HashOps {
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

    WriteResult<Long> hset(byte[] keyBytes, List<byte[]> fieldValuePairs);

    WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields);
}
