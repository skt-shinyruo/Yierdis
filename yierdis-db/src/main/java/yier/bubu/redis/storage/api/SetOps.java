package yier.bubu.redis.storage.api;

import java.util.List;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public interface SetOps {
    ByteSequenceSource smembers(byte[] keyBytes);

    boolean sismember(byte[] keyBytes, byte[] member);

    long scard(byte[] keyBytes);

    CollectionScanWindow sscan(byte[] keyBytes, ScanCursorV2 cursor, byte[] globPattern, int count);

    WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members);

    WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members);
}
