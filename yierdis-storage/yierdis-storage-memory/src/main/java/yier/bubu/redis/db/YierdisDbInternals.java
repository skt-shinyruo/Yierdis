package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.MemoryLedger;

interface YierdisDbInternals {
    void checkThread();

    <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan);

    YierdisDbKeyLifecycle keyLifecycle();

    MemoryLedger ledger();
}
