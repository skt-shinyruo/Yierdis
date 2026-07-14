package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

public interface YierdisDbInternals {
    void checkThread();

    <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan);

    default EntryRecord liveEntryRecord(KeyHandle keyHandle) {
        return keyLifecycle().liveEntryRecord(keyHandle);
    }

    boolean reclaimExpired(KeyHandle keyHandle, EntryRecord expectedRecord, long nowMillis);

    boolean evict(KeyHandle keyHandle, EntryRecord expectedRecord);

    YierdisDbKeyLifecycle keyLifecycle();

    MemoryLedger ledger();
}
