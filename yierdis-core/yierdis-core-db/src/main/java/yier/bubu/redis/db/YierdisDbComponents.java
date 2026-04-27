package yier.bubu.redis.db;

import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.MemoryOps;

final class YierdisDbComponents {
    final YierdisDbStorageComponents storage;
    final YierdisDbConfig config;
    final YierdisDbMemoryEstimator memoryEstimator;
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

    YierdisDbComponents(
            YierdisDbStorageComponents storage,
            YierdisDbConfig config,
            YierdisDbMemoryEstimator memoryEstimator,
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
            DbLifecycleOps lifecycleOps
    ) {
        this.storage = storage;
        this.config = config;
        this.memoryEstimator = memoryEstimator;
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
    }
}
