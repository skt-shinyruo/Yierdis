package yier.bubu.redis.storage.api;

// ZSetReadOps：zset 只读能力边界。

import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequence;

public interface ZSetReadOps {
    MeasuredBulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores);

    MeasuredBulkStringSequence zrevrange(byte[] keyBytes, long start, long stop, boolean withScores);

    MeasuredBulkStringSequence zrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );

    MeasuredBulkStringSequence zrevrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );
}
