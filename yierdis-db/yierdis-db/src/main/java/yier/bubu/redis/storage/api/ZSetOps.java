package yier.bubu.redis.storage.api;

import java.util.List;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public interface ZSetOps {
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

    WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs);

    WriteResult<Long> zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive);

    WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop);

    WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members);
}
