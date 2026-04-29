package yier.bubu.redis.ops;

// ZSetWriteOps：zset 写能力边界。

import java.util.List;

public interface ZSetWriteOps {
    long zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs);

    long zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive);

    long zremrangeByRank(byte[] keyBytes, long start, long stop);

    long zrem(byte[] keyBytes, List<byte[]> members);
}
