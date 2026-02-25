package yier.bubu.redis.ops;

// ZSetOps：zset 类型操作边界。

import yier.bubu.redis.ops.result.BulkStringSequence;

import java.util.List;

public interface ZSetOps {
    long zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs);

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

    long zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive);

    long zremrangeByRank(byte[] keyBytes, long start, long stop);

    long zrem(byte[] keyBytes, List<byte[]> members);
}
