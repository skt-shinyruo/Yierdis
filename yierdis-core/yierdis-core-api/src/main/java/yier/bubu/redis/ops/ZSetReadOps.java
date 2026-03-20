package yier.bubu.redis.ops;

// ZSetReadOps：zset 只读能力边界。

import yier.bubu.redis.ops.result.BulkStringSequence;

public interface ZSetReadOps {
    BulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores);

    BulkStringSequence zrevrange(byte[] keyBytes, long start, long stop, boolean withScores);

    BulkStringSequence zrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );

    BulkStringSequence zrevrangeByScore(
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
