package yier.bubu.redis.storage.api;

import yier.bubu.redis.bytes.BytesView;

public interface TtlOps {
    long ttlSeconds(BytesView keyView);

    long ttlMillis(BytesView keyView);

    default WriteResult<Boolean> expire(BytesView keyView, long seconds) {
        return expire(keyView, seconds, ExpireCondition.NONE);
    }

    WriteResult<Boolean> expire(BytesView keyView, long seconds, ExpireCondition condition);

    default WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds) {
        return pexpire(keyView, milliseconds, ExpireCondition.NONE);
    }

    WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds, ExpireCondition condition);

    default WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds) {
        return expireAtSeconds(keyView, unixSeconds, ExpireCondition.NONE);
    }

    WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds, ExpireCondition condition);

    default WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis) {
        return expireAtMillis(keyView, unixMillis, ExpireCondition.NONE);
    }

    WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis, ExpireCondition condition);

    WriteResult<Boolean> persist(BytesView keyView);
}
