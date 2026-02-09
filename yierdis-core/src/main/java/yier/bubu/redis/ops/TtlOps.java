package yier.bubu.redis.ops;

// TtlOps：TTL/过期能力边界（EXPIRE/PEXPIRE/EXPIREAT/PERSIST/TTL/PTTL 等）。

import yier.bubu.redis.db.YierdisBytesView;

public interface TtlOps {
    boolean expire(YierdisBytesView keyView, long seconds);

    boolean pexpire(YierdisBytesView keyView, long milliseconds);

    boolean expireAtSeconds(YierdisBytesView keyView, long unixSeconds);

    boolean expireAtMillis(YierdisBytesView keyView, long unixMillis);

    boolean persist(YierdisBytesView keyView);

    long ttlSeconds(YierdisBytesView keyView);

    long ttlMillis(YierdisBytesView keyView);
}

