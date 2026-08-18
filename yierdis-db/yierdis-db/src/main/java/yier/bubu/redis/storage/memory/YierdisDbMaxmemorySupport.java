package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Objects;
import java.util.function.LongSupplier;

final class YierdisDbMaxmemorySupport {
    private final YierdisDbKernel kernel;
    private final YierdisDbMemoryContext memoryContext;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final LongSupplier usedBytesForMaxmemory;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitNanos;

    YierdisDbMaxmemorySupport(
            YierdisDbKernel kernel,
            YierdisDbMemoryContext memoryContext,
            YierdisDbKeyLifecycle keyLifecycle,
            LongSupplier usedBytesForMaxmemory,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitNanos
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.usedBytesForMaxmemory = Objects.requireNonNull(usedBytesForMaxmemory, "usedBytesForMaxmemory");
        this.maxmemoryPolicy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        this.maxmemorySamples = maxmemorySamples;
        this.evictionTimeLimitNanos = evictionTimeLimitNanos;
    }

    void evictUntilUnder(long limitBytes) {
        kernel.checkOwner();
        evictUntilUnderChecked(limitBytes);
    }

    private void evictUntilUnderChecked(long requestedLimitBytes) {
        long limitBytes = requestedLimitBytes;
        if (limitBytes < 0) {
            limitBytes = 0;
        }
        trimEmptyNativePages();
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
            AllocatorKeyHandle victim = pickEvictionKey(nowMillis);
            if (victim == null) {
                break;
            }
            EntryRecord record = keyLifecycle.entryRecord(victim);
            if (record == null) {
                continue;
            }
            if (kernel.reclaimExpired(victim, record, nowMillis)) {
                trimEmptyNativePages();
                if (usedBytesForMaxmemory() <= limitBytes) {
                    return;
                }
                continue;
            }
            if (kernel.evict(victim, record)) {
                trimEmptyNativePages();
            }
        }
        trimEmptyNativePages();
        if (usedBytesForMaxmemory() <= limitBytes) {
            return;
        }
    }

    MaxmemoryCandidate sampleCandidate(
            MaxmemoryParticipant owner,
            MaxmemoryPolicy policy,
            long nowMillis
    ) {
        kernel.checkOwner();
        Objects.requireNonNull(owner, "owner");
        if (policy == null || policy == MaxmemoryPolicy.NOEVICTION) {
            return null;
        }
        if (keyLifecycle.keyCount() == 0) {
            return null;
        }

        AllocatorKeyHandle keyHandle = keyLifecycle.randomKeyHandle();
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
        return new MaxmemoryCandidate(owner, keyHandle, lruClock);
    }

    MaxmemoryCandidate scanBestCandidate(
            MaxmemoryParticipant owner,
            MaxmemoryPolicy policy,
            long nowMillis
    ) {
        kernel.checkOwner();
        Objects.requireNonNull(owner, "owner");
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

        AllocatorKeyHandle bestKeyHandle = best.keyHandle();
        if (bestKeyHandle == null) {
            return null;
        }
        return new MaxmemoryCandidate(owner, bestKeyHandle, best.lru());
    }

    boolean evict(MaxmemoryParticipant owner, MaxmemoryCandidate candidate, long nowMillis) {
        kernel.checkOwner();
        if (candidate == null || candidate.owner() != owner) {
            return false;
        }

        if (!(candidate.keyHandle() instanceof AllocatorKeyHandle key)) {
            return false;
        }
        EntryRecord record = keyLifecycle.entryRecord(key);
        if (record == null) {
            return false;
        }
        if (kernel.reclaimExpired(key, record, nowMillis)) {
            return true;
        }
        return kernel.evict(key, record);
    }

    private AllocatorKeyHandle pickEvictionKey(long nowMillis) {
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
        AllocatorKeyHandle bestKey = null;
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
            AllocatorKeyHandle key = keyLifecycle.randomKeyHandle();
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

    private long usedBytesForMaxmemory() {
        return usedBytesForMaxmemory.getAsLong();
    }

    private void trimEmptyNativePages() {
        memoryContext.trimEmptyNativePages(MemoryPressureBudget.UNLIMITED);
    }

    private static final class BestLruCandidate {
        private AllocatorKeyHandle keyHandle;
        private long lru = Long.MAX_VALUE;

        void consider(AllocatorKeyHandle keyHandle, EntryRecord record) {
            if (keyHandle == null || record == null) {
                return;
            }
            long candidateLru = record.lruOrLfu();
            if (this.keyHandle == null || candidateLru < lru) {
                this.keyHandle = keyHandle;
                this.lru = candidateLru;
            }
        }

        AllocatorKeyHandle keyHandle() {
            return keyHandle;
        }

        long lru() {
            return lru;
        }
    }
}
