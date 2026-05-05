package yier.bubu.redis.ops;

// TtlWriteOps：TTL 写能力边界。

import yier.bubu.redis.bytes.BytesView;

public interface TtlWriteOps {
    WriteResult<Boolean> expire(BytesView keyView, long seconds);

    WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds);

    WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds);

    WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis);

    WriteResult<Boolean> persist(BytesView keyView);
}
