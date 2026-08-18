package yier.bubu.redis.storage.api;

import yier.bubu.redis.bytes.BytesView;

public interface TtlOps {
    long ttlSeconds(BytesView keyView);

    long ttlMillis(BytesView keyView);

    WriteResult<Boolean> expire(BytesView keyView, long seconds);

    WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds);

    WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds);

    WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis);

    WriteResult<Boolean> persist(BytesView keyView);
}
