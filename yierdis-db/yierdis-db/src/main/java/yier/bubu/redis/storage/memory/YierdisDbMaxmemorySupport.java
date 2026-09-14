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
            // 过期 key 必须作为可回收候选上报，而不是跳过：lruClock=0 让它在 LRU 比较中先于任何
            // live key（live 访问时钟恒 >= 1）被选中，evict(...) 会走 expiration reclamation 回收它。
            return new MaxmemoryCandidate(owner, keyHandle, 0L);
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

        VictimPick pick = pickFullScanVictim(nowMillis);
        if (pick == null) {
            return null;
        }
        // 与 sampleCandidate 同一口径：过期 key 是最优可回收候选，lruClock=0 排在所有 live key 之前。
        return new MaxmemoryCandidate(owner, pick.keyHandle(), pick.expired() ? 0L : pick.lru());
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
            VictimPick pick = pickFullScanVictim(nowMillis);
            return pick == null ? null : pick.keyHandle();
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
                // 抽到过期 key 直接作为 reclaim victim（与 ALLKEYS_RANDOM 已具备的能力一致）。
                return key;
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

    // 全量扫描的 victim 选择：过期 key 永远先于 live victim（跳过它们会让「只剩过期条目」的
    // keyspace 无故 OOM）；没有过期 key 时退化为最小 LRU clock 的 live key。
    private VictimPick pickFullScanVictim(long nowMillis) {
        BestLruCandidate best = new BestLruCandidate();
        AllocatorKeyHandle[] expiredKey = new AllocatorKeyHandle[1];
        keyLifecycle.forEachKeyHandle((k, record) -> {
            if (k == null || record == null) {
                return;
            }
            if (keyLifecycle.isKeyExpired(k, nowMillis)) {
                if (expiredKey[0] == null) {
                    expiredKey[0] = k;
                }
                return;
            }
            best.consider(k, record);
        });
        if (expiredKey[0] != null) {
            return new VictimPick(expiredKey[0], 0L, true);
        }
        AllocatorKeyHandle bestKeyHandle = best.keyHandle();
        return bestKeyHandle == null ? null : new VictimPick(bestKeyHandle, best.lru(), false);
    }

    private record VictimPick(AllocatorKeyHandle keyHandle, long lru, boolean expired) {
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
