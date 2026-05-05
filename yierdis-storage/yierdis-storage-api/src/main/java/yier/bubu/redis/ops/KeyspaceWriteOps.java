package yier.bubu.redis.ops;

// KeyspaceWriteOps：keyspace 写能力边界。

import java.util.Collection;

public interface KeyspaceWriteOps {
    WriteResult<Long> del(Collection<byte[]> keys);
}
