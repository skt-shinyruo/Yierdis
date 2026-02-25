package yier.bubu.redis.ops;

// TtlOps：TTL/过期能力边界（EXPIRE/PEXPIRE/EXPIREAT/PERSIST/TTL/PTTL 等）。

import yier.bubu.redis.bytes.BytesView;

public interface TtlOps {
    boolean expire(BytesView keyView, long seconds);

    boolean pexpire(BytesView keyView, long milliseconds);

    boolean expireAtSeconds(BytesView keyView, long unixSeconds);

    boolean expireAtMillis(BytesView keyView, long unixMillis);

    boolean persist(BytesView keyView);

    long ttlSeconds(BytesView keyView);

    long ttlMillis(BytesView keyView);
}

