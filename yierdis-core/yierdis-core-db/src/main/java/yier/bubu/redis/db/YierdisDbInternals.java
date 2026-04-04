package yier.bubu.redis.db;

import yier.bubu.redis.db.key.KeyHandle;

interface YierdisDbInternals {
    void checkThread();

    <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan);

    YierdisDbKeyLifecycle keyLifecycle();

    void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object);

    void adjustUsedBytes(long deltaBytes);
}
