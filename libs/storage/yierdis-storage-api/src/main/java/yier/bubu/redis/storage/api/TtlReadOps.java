package yier.bubu.redis.storage.api;

// TtlReadOps：TTL 只读能力边界。

import yier.bubu.redis.bytes.BytesView;

public interface TtlReadOps {
    long ttlSeconds(BytesView keyView);

    long ttlMillis(BytesView keyView);
}
