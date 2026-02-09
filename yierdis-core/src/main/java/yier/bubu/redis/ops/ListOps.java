package yier.bubu.redis.ops;

// ListOps：list 类型操作边界（command 层通过该接口访问 list 行为）。

import yier.bubu.redis.protocol.ReplySink;

import java.util.List;

public interface ListOps {
    long lpush(byte[] keyBytes, List<byte[]> values);

    long rpush(byte[] keyBytes, List<byte[]> values);

    int lrangeCount(byte[] keyBytes, int start, int stop);

    void lrangeWriteTo(byte[] keyBytes, int start, int stop, ReplySink out);

    List<byte[]> lpop(byte[] keyBytes, int count);

    List<byte[]> rpop(byte[] keyBytes, int count);
}
