package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;

public interface YierdisDbInternals {
    void checkThread();

    <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan);

    YierdisDbKeyLifecycle keyLifecycle();

    MemoryLedger ledger();
}
