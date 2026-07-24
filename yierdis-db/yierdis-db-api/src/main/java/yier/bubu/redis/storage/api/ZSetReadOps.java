package yier.bubu.redis.storage.api;

// ZSetReadOps：zset 只读能力边界。

import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;

public interface ZSetReadOps {
    ByteSequenceSource zrange(byte[] keyBytes, long start, long stop, boolean withScores);

    ByteSequenceSource zrevrange(byte[] keyBytes, long start, long stop, boolean withScores);

    ByteSequenceSource zrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );

    ByteSequenceSource zrevrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );

    CollectionScanWindow zscan(byte[] keyBytes, ScanCursorV2 cursor, byte[] globPattern, int count);
}
