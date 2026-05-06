package yier.bubu.redis.ops;

// ZSetWriteOps：zset 写能力边界。

import java.util.List;

public interface ZSetWriteOps {
    WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs);

    WriteResult<Long> zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive);

    WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop);

    WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members);
}
