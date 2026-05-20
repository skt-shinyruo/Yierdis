package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.api.MemoryOps;

public final class YierdisDbComponents {
    final YierdisDbRuntimeState runtimeState;
    final YierdisDbStorageComponents storage;
    final EntryTable entries;
    final NativeKeyDirectory keyDirectory;
    final YierdisDbConfig config;
    final YierdisDbMemoryLedger ledger;
    final YierdisDbMutationExecutor mutationExecutor;
    final YierdisDbExpirationSupport expirationSupport;
    final YierdisDbMaxmemorySupport maxmemorySupport;
    final YierdisDbKeyLifecycle keyLifecycle;
    final YierdisDbInternals internals;
    final YierdisStringOps stringOps;
    final YierdisHashOps hashOps;
    final YierdisListOps listOps;
    final YierdisSetOps setOps;
    final YierdisZSetOps zsetOps;
    final YierdisHllOps hllOps;
    final YierdisTtlOps ttlOps;
    final YierdisKeyspaceOps keyspaceOps;
    final YierdisDbMemoryReporter memoryReporter;
    final YierdisDbIntrospection introspection;
    final DbReads reads;
    final DbWrites writes;
    final ExpirationManager expirationManager;
    final MemoryOps memoryOps;
    final DbLifecycleOps lifecycleOps;
    final YierdisDbDataMaintenance maintenance;

    YierdisDbComponents(
            YierdisDbRuntimeState runtimeState,
            YierdisDbStorageComponents storage,
            EntryTable entries,
            NativeKeyDirectory keyDirectory,
            YierdisDbConfig config,
            YierdisDbMemoryLedger ledger,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbExpirationSupport expirationSupport,
            YierdisDbMaxmemorySupport maxmemorySupport,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbInternals internals,
            YierdisStringOps stringOps,
            YierdisHashOps hashOps,
            YierdisListOps listOps,
            YierdisSetOps setOps,
            YierdisZSetOps zsetOps,
            YierdisHllOps hllOps,
            YierdisTtlOps ttlOps,
            YierdisKeyspaceOps keyspaceOps,
            YierdisDbMemoryReporter memoryReporter,
            YierdisDbIntrospection introspection,
            DbReads reads,
            DbWrites writes,
            ExpirationManager expirationManager,
            MemoryOps memoryOps,
            DbLifecycleOps lifecycleOps,
            YierdisDbDataMaintenance maintenance
    ) {
        this.runtimeState = runtimeState;
        this.storage = storage;
        this.entries = entries;
        this.keyDirectory = keyDirectory;
        this.config = config;
        this.ledger = ledger;
        this.mutationExecutor = mutationExecutor;
        this.expirationSupport = expirationSupport;
        this.maxmemorySupport = maxmemorySupport;
        this.keyLifecycle = keyLifecycle;
        this.internals = internals;
        this.stringOps = stringOps;
        this.hashOps = hashOps;
        this.listOps = listOps;
        this.setOps = setOps;
        this.zsetOps = zsetOps;
        this.hllOps = hllOps;
        this.ttlOps = ttlOps;
        this.keyspaceOps = keyspaceOps;
        this.memoryReporter = memoryReporter;
        this.introspection = introspection;
        this.reads = reads;
        this.writes = writes;
        this.expirationManager = expirationManager;
        this.memoryOps = memoryOps;
        this.lifecycleOps = lifecycleOps;
        this.maintenance = maintenance;
    }
}
