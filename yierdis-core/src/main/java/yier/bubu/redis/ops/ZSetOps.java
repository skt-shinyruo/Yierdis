package yier.bubu.redis.ops;

// ZSetOps：zset 类型操作边界。

import yier.bubu.redis.db.YierdisBulkStringOutput;

import java.util.List;

public interface ZSetOps {
    long zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs);

    int zrangeReplyCount(byte[] keyBytes, long start, long stop, boolean withScores);

    void zrangeReplyInto(byte[] keyBytes, long start, long stop, boolean withScores, YierdisBulkStringOutput out);

    int zrevrangeReplyCount(byte[] keyBytes, long start, long stop, boolean withScores);

    void zrevrangeReplyInto(byte[] keyBytes, long start, long stop, boolean withScores, YierdisBulkStringOutput out);

    int zrangeByScoreReplyCount(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );

    void zrangeByScoreReplyInto(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            YierdisBulkStringOutput out
    );

    int zrevrangeByScoreReplyCount(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );

    void zrevrangeByScoreReplyInto(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            YierdisBulkStringOutput out
    );

    long zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive);

    long zremrangeByRank(byte[] keyBytes, long start, long stop);

    long zrem(byte[] keyBytes, List<byte[]> members);
}
