package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

final class YierdisDbMaxmemorySupport implements MaxmemoryParticipant {
    private final YierdisDbKernel kernel;
    private final LongSupplier usedBytesForMaxmemory;
    private final Supplier<MemoryUsageSnapshot> memoryUsageSupplier;
    private final LongConsumer cleanupExpired;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitNanos;

    YierdisDbMaxmemorySupport(
            YierdisDbKernel kernel,
            LongSupplier usedBytesForMaxmemory,
            Supplier<MemoryUsageSnapshot> memoryUsageSupplier,
            LongConsumer cleanupExpired,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitNanos
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.usedBytesForMaxmemory = Objects.requireNonNull(usedBytesForMaxmemory, "usedBytesForMaxmemory");
        this.memoryUsageSupplier = Objects.requireNonNull(memoryUsageSupplier, "memoryUsageSupplier");
        this.cleanupExpired = Objects.requireNonNull(cleanupExpired, "cleanupExpired");
        this.maxmemoryPolicy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        this.maxmemorySamples = maxmemorySamples;
        this.evictionTimeLimitNanos = evictionTimeLimitNanos;
    }

    void evictUntilUnder(long limitBytes) {
        kernel.maintain(scope -> {
            evictUntilUnder(scope, limitBytes);
            return null;
        });
    }

    private void evictUntilUnder(MaintenanceScope scope, long requestedLimitBytes) {
        long limitBytes = requestedLimitBytes;
        if (limitBytes < 0) {
            limitBytes = 0;
        }
        scope.trimEmptyNativePages();
        if (usedBytesForMaxmemory() <= limitBytes) {
            return;
        }

        int attempts = 0;
        int maxAttempts = Math.max(64, scope.keyCount() * 2);
        long nowMillis = System.currentTimeMillis();
        long deadline = System.nanoTime() + evictionTimeLimitNanos;
        // 维护任务在调用线程内执行，必须同时用时间窗口和尝试次数限制淘汰循环，避免一次写入拖垮 event loop。
        while (usedBytesForMaxmemory() > limitBytes && attempts++ < maxAttempts) {
            if (System.nanoTime() >= deadline) {
                break;
            }
            KeyHandle victim = pickEvictionKey(scope, nowMillis);
            if (victim == null) {
                break;
            }
            EntryRecord record = scope.entryRecord(victim);
            if (record == null) {
                continue;
            }
            if (scope.reclaimExpired(victim, record, nowMillis)) {
                scope.trimEmptyNativePages();
                if (usedBytesForMaxmemory() <= limitBytes) {
                    return;
                }
                continue;
            }
            if (scope.evict(victim, record)) {
                scope.trimEmptyNativePages();
            }
        }
        scope.trimEmptyNativePages();
        if (usedBytesForMaxmemory() <= limitBytes) {
            return;
        }
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        return kernel.maintain(ignored -> memoryUsageSupplier.get());
    }

    public long usedBytesForMaxmemory() {
        return kernel.maintain(ignored -> usedBytesForMaxmemory.getAsLong());
    }

    @Override
    public int keyCountEstimate() {
        return kernel.maintain(scope -> Math.max(0, scope.keyCount()));
    }

    @Override
    public void cleanupExpired(long nowMillis) {
        kernel.maintain(ignored -> {
            cleanupExpired.accept(nowMillis);
            return null;
        });
    }

    @Override
    public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
        return kernel.maintain(scope -> sampleCandidate(scope, policy, nowMillis));
    }

    private MaxmemoryCandidate sampleCandidate(
            MaintenanceScope scope,
            MaxmemoryPolicy policy,
            long nowMillis
    ) {
        if (policy == null || policy == MaxmemoryPolicy.NOEVICTION) {
            return null;
        }
        if (scope.keyCount() == 0) {
            return null;
        }

        KeyHandle keyHandle = scope.randomKeyHandle();
        if (keyHandle == null) {
            return null;
        }
        EntryRecord record = scope.entryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        if (scope.isKeyExpired(keyHandle, nowMillis)) {
            return null;
        }

        long lruClock = policy == MaxmemoryPolicy.ALLKEYS_LRU ? record.lruOrLfu() : 0L;
        return new MaxmemoryCandidate(this, keyHandle, lruClock);
    }

    @Override
    public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
        return kernel.maintain(scope -> scanBestCandidate(scope, policy, nowMillis));
    }

    private MaxmemoryCandidate scanBestCandidate(
            MaintenanceScope scope,
            MaxmemoryPolicy policy,
            long nowMillis
    ) {
        if (policy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }
        if (scope.keyCount() == 0) {
            return null;
        }

        BestLruCandidate best = new BestLruCandidate();
        scope.forEachKeyHandle((k, record) -> {
            if (k == null || record == null) {
                return;
            }
            if (scope.isKeyExpired(k, nowMillis)) {
                return;
            }
            best.consider(k, record);
        });

        KeyHandle bestKeyHandle = best.keyHandle();
        if (bestKeyHandle == null) {
            return null;
        }
        return new MaxmemoryCandidate(this, bestKeyHandle, best.lru());
    }

    @Override
    public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
        return kernel.maintain(scope -> evict(scope, candidate, nowMillis));
    }

    private boolean evict(MaintenanceScope scope, MaxmemoryCandidate candidate, long nowMillis) {
        if (candidate == null || candidate.owner() != this) {
            return false;
        }

        if (!(candidate.keyHandle() instanceof KeyHandle key)) {
            return false;
        }
        EntryRecord record = scope.entryRecord(key);
        if (record == null) {
            return false;
        }
        if (scope.reclaimExpired(key, record, nowMillis)) {
            return true;
        }
        return scope.evict(key, record);
    }

    private KeyHandle pickEvictionKey(MaintenanceScope scope, long nowMillis) {
        if (scope.keyCount() == 0) {
            return null;
        }

        if (maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_RANDOM) {
            return scope.randomKeyHandle();
        }

        if (maxmemoryPolicy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }

        int total = scope.keyCount();
        KeyHandle bestKey = null;
        long bestLru = Long.MAX_VALUE;
        int samples = Math.max(1, maxmemorySamples);

        if (samples >= total) {
            // 样本数覆盖全量时退化为完整扫描，避免随机抽样在小 keyspace 上错过最旧 key。
            BestLruCandidate best = new BestLruCandidate();
            scope.forEachKeyHandle((k, record) -> {
                if (scope.isKeyExpired(k, nowMillis)) {
                    return;
                }
                if (record == null) {
                    return;
                }
                best.consider(k, record);
            });
            return best.keyHandle();
        }

        for (int i = 0; i < samples; i++) {
            KeyHandle key = scope.randomKeyHandle();
            if (key == null) {
                break;
            }
            EntryRecord record = scope.entryRecord(key);
            if (record == null) {
                continue;
            }
            if (scope.isKeyExpired(key, nowMillis)) {
                continue;
            }
            long lru = record.lruOrLfu();
            if (bestKey == null || lru < bestLru) {
                bestKey = key;
                bestLru = lru;
            }
        }
        return bestKey;
    }

    private static final class BestLruCandidate {
        private KeyHandle keyHandle;
        private long lru = Long.MAX_VALUE;

        void consider(KeyHandle keyHandle, EntryRecord record) {
            if (keyHandle == null || record == null) {
                return;
            }
            long candidateLru = record.lruOrLfu();
            if (this.keyHandle == null || candidateLru < lru) {
                this.keyHandle = keyHandle;
                this.lru = candidateLru;
            }
        }

        KeyHandle keyHandle() {
            return keyHandle;
        }

        long lru() {
            return lru;
        }
    }
}
