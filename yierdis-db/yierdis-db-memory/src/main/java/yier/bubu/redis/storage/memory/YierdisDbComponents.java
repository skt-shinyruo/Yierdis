package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;

record YierdisDbComponents(
        YierdisDbHealth health,
        DbComponentMemoryUsage memoryUsage,
        YierdisDbMemoryLedger ledger,
        YierdisDbKeyLifecycle keyLifecycle,
        YierdisDbIntrospection introspection,
        YierdisDbMemoryReporter memoryReporter,
        YierdisDbDataMaintenance maintenance,
        DbReads reads,
        DbWrites writes,
        DbLifecycleOps lifecycleOps
) {
}
