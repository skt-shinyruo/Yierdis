package yier.bubu.redis.ops;

// ZSetOps：zset 类型操作边界。

import yier.bubu.redis.protocol.ReplySink;

import java.util.List;

public interface ZSetOps {
    long zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs);

    int zrangeCount(byte[] keyBytes, long start, long stop, boolean withScores);

    void zrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, ReplySink out);

    int zrevrangeCount(byte[] keyBytes, long start, long stop, boolean withScores);

    void zrevrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, ReplySink out);

    int zrangeByScoreCount(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );

    void zrangeByScoreWriteTo(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            ReplySink out
    );

    int zrevrangeByScoreCount(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    );

    void zrevrangeByScoreWriteTo(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            ReplySink out
    );

    long zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive);

    long zremrangeByRank(byte[] keyBytes, long start, long stop);

    long zrem(byte[] keyBytes, List<byte[]> members);
}
