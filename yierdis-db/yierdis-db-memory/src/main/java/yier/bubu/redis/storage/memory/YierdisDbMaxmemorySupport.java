package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class YierdisDbMaxmemorySupport implements MaxmemoryParticipant {
    private final Runnable threadChecker;
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final LongSupplier usedBytesForMaxmemory;
    private final Supplier<MemoryUsageSnapshot> memoryUsageSupplier;
    private final LongConsumer cleanupExpired;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitNanos;

    YierdisDbMaxmemorySupport(
            Runnable threadChecker,
            YierdisDbInternals internals,
            YierdisDbKeyLifecycle keyLifecycle,
            LongSupplier usedBytesForMaxmemory,
            Supplier<MemoryUsageSnapshot> memoryUsageSupplier,
            LongConsumer cleanupExpired,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitNanos
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.usedBytesForMaxmemory = Objects.requireNonNull(usedBytesForMaxmemory, "usedBytesForMaxmemory");
        this.memoryUsageSupplier = Objects.requireNonNull(memoryUsageSupplier, "memoryUsageSupplier");
        this.cleanupExpired = Objects.requireNonNull(cleanupExpired, "cleanupExpired");
        this.maxmemoryPolicy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        this.maxmemorySamples = maxmemorySamples;
        this.evictionTimeLimitNanos = evictionTimeLimitNanos;
    }

    void evictUntilUnder(long limitBytes) {
        threadChecker.run();
        if (limitBytes < 0) {
            limitBytes = 0;
        }
        keyLifecycle.stableMemoryBackend().trimEmptyPages(MemoryPressureBudget.unlimited());
        if (usedBytesForMaxmemory() <= limitBytes) {
            return;
        }

        int attempts = 0;
        int maxAttempts = Math.max(64, keyLifecycle.keyCount() * 2);
        long nowMillis = System.currentTimeMillis();
        long deadline = System.nanoTime() + evictionTimeLimitNanos;
        // 维护任务在调用线程内执行，必须同时用时间窗口和尝试次数限制淘汰循环，避免一次写入拖垮 event loop。
        while (usedBytesForMaxmemory() > limitBytes && attempts++ < maxAttempts) {
            if (System.nanoTime() >= deadline) {
                break;
            }
            KeyHandle victim = pickEvictionKey(nowMillis);
            if (victim == null) {
                break;
            }
            EntryRecord record = keyLifecycle.entryRecord(victim);
            if (record == null) {
                continue;
            }
            if (internals.reclaimExpired(victim, record, nowMillis)) {
                keyLifecycle.stableMemoryBackend().trimEmptyPages(MemoryPressureBudget.unlimited());
                if (usedBytesForMaxmemory() <= limitBytes) {
                    return;
                }
                continue;
            }
            if (internals.evict(victim, record)) {
                keyLifecycle.stableMemoryBackend().trimEmptyPages(MemoryPressureBudget.unlimited());
            }
        }
        keyLifecycle.stableMemoryBackend().trimEmptyPages(MemoryPressureBudget.unlimited());
        if (usedBytesForMaxmemory() <= limitBytes) {
            return;
        }
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        threadChecker.run();
        return memoryUsageSupplier.get();
    }

    @Override
    public long usedBytesForMaxmemory() {
        threadChecker.run();
        return usedBytesForMaxmemory.getAsLong();
    }

    @Override
    public int keyCountEstimate() {
        threadChecker.run();
        return Math.max(0, keyLifecycle.keyCount());
    }

    @Override
    public void cleanupExpired(long nowMillis) {
        threadChecker.run();
        cleanupExpired.accept(nowMillis);
    }

    @Override
    public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
        threadChecker.run();
        if (policy == null || policy == MaxmemoryPolicy.NOEVICTION) {
            return null;
        }
        if (keyLifecycle.keyCount() == 0) {
            return null;
        }

        KeyHandle keyHandle = keyLifecycle.randomKeyHandle();
        if (keyHandle == null) {
            return null;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return null;
        }

        long lruClock = policy == MaxmemoryPolicy.ALLKEYS_LRU ? record.lruOrLfu() : 0L;
        return new MaxmemoryCandidate(this, keyHandle, lruClock);
    }

    @Override
    public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
        threadChecker.run();
        if (policy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }
        if (keyLifecycle.keyCount() == 0) {
            return null;
        }

        BestLruCandidate best = new BestLruCandidate();
        keyLifecycle.forEachKeyHandle((k, record) -> {
            if (k == null || record == null) {
                return;
            }
            if (keyLifecycle.isKeyExpired(k, nowMillis)) {
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
        threadChecker.run();
        if (candidate == null || candidate.owner() != this) {
            return false;
        }

        if (!(candidate.keyHandle() instanceof KeyHandle key)) {
            return false;
        }
        EntryRecord record = keyLifecycle.entryRecord(key);
        if (record == null) {
            return false;
        }
        if (internals.reclaimExpired(key, record, nowMillis)) {
            return true;
        }
        return internals.evict(key, record);
    }

    private KeyHandle pickEvictionKey(long nowMillis) {
        if (keyLifecycle.keyCount() == 0) {
            return null;
        }

        if (maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_RANDOM) {
            return keyLifecycle.randomKeyHandle();
        }

        if (maxmemoryPolicy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }

        int total = keyLifecycle.keyCount();
        KeyHandle bestKey = null;
        long bestLru = Long.MAX_VALUE;
        int samples = Math.max(1, maxmemorySamples);

        if (samples >= total) {
            // 样本数覆盖全量时退化为完整扫描，避免随机抽样在小 keyspace 上错过最旧 key。
            BestLruCandidate best = new BestLruCandidate();
            keyLifecycle.forEachKeyHandle((k, record) -> {
                if (keyLifecycle.isKeyExpired(k, nowMillis)) {
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
            KeyHandle key = keyLifecycle.randomKeyHandle();
            if (key == null) {
                break;
            }
            EntryRecord record = keyLifecycle.entryRecord(key);
            if (record == null) {
                continue;
            }
            if (keyLifecycle.isKeyExpired(key, nowMillis)) {
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
