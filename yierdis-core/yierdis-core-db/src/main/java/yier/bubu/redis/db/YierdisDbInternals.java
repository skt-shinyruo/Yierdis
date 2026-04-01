package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

interface YierdisDbInternals {
    void checkThread();

    OffHeapAllocator offHeapAllocator();

    YierdisKeyspace<YierdisObject> store();

    YierdisExpireIndex expires();

    <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan);

    void touch(YierdisObject object);

    void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object);

    YierdisObject getObjectIfNotExpired(byte[] keyBytes);

    YierdisObject getObjectIfNotExpired(BytesView keyView);

    YierdisObject getObjectIfNotExpired(KeyHandle keyHandle);

    boolean removeIfExpired(byte[] keyBytes, YierdisObject object, long nowMillis);

    boolean removeIfExpired(KeyHandle keyHandle, YierdisObject object, long nowMillis);

    boolean isKeyExpired(KeyHandle keyHandle, long nowMillis);

    void setExpireAtMillis(byte[] keyBytes, long expireAtMillis);

    void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis);

    void removeExpire(byte[] keyBytes);

    void removeExpire(KeyHandle keyHandle);

    void adjustUsedBytes(long deltaBytes);
}
