package yier.bubu.redis.ops;

// ListWriteOps：list 写能力边界。

import java.util.List;

public interface ListWriteOps {
    long lpush(byte[] keyBytes, List<byte[]> values);

    long rpush(byte[] keyBytes, List<byte[]> values);

    List<byte[]> lpop(byte[] keyBytes, int count);

    List<byte[]> rpop(byte[] keyBytes, int count);
}
