package yier.bubu.redis.ops;

// TtlWriteOps：TTL 写能力边界。

import yier.bubu.redis.bytes.BytesView;

public interface TtlWriteOps {
    boolean expire(BytesView keyView, long seconds);

    boolean pexpire(BytesView keyView, long milliseconds);

    boolean expireAtSeconds(BytesView keyView, long unixSeconds);

    boolean expireAtMillis(BytesView keyView, long unixMillis);

    boolean persist(BytesView keyView);
}
