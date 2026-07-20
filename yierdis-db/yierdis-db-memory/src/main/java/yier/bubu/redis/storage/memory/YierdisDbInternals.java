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
import java.util.function.Supplier;
import yier.bubu.redis.common.command.MutationContext;

public interface YierdisDbInternals {
    void checkThread();

    <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan);

    <T> T withMutationContext(MutationContext context, Supplier<T> action);

    default EntryRecord liveEntryRecord(KeyHandle keyHandle) {
        return keyLifecycle().liveEntryRecord(keyHandle);
    }

    boolean reclaimExpired(KeyHandle keyHandle, EntryRecord expectedRecord, long nowMillis);

    boolean evict(KeyHandle keyHandle, EntryRecord expectedRecord);

    YierdisDbKeyLifecycle keyLifecycle();

    MemoryLedger ledger();
}
