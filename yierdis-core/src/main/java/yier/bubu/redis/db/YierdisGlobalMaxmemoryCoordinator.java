package yier.bubu.redis.db;

// YierdisGlobalMaxmemoryCoordinator：多 DB 场景下的实例级 maxmemory 协调器（best-effort，尽量对齐 Redis 全实例预算语义）。

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

final class YierdisGlobalMaxmemoryCoordinator {
    private final YierdisDb[] dbs;
    private final YierdisOffHeapAllocator offHeapAllocator;
    private final long maxmemoryBytes;
    private final YierdisDb.MaxmemoryPolicy policy;
    private final int samples;
    private final long evictionTimeLimitNanos;
    private final AtomicLong globalLruClock = new AtomicLong(0);

    YierdisGlobalMaxmemoryCoordinator(
            YierdisDb[] dbs,
            YierdisOffHeapAllocator offHeapAllocator,
            long maxmemoryBytes,
            YierdisDb.MaxmemoryPolicy policy,
            int samples,
            long evictionTimeLimitNanos
    ) {
        this.dbs = Objects.requireNonNull(dbs, "dbs");
        this.offHeapAllocator = offHeapAllocator;
        this.maxmemoryBytes = Math.max(0, maxmemoryBytes);
        this.policy = policy == null ? YierdisDb.MaxmemoryPolicy.NOEVICTION : policy;
        this.samples = Math.max(1, samples);
        this.evictionTimeLimitNanos = Math.max(0, evictionTimeLimitNanos);
    }

    long nextLruClock() {
        return globalLruClock.incrementAndGet();
    }

    void ensureWriteAllowed(long additionalBytes) {
        if (maxmemoryBytes <= 0) {
            return;
        }
        if (policy != YierdisDb.MaxmemoryPolicy.NOEVICTION) {
            return;
        }
        long extra = Math.max(0, additionalBytes);
        if (globalUsedBytesForMaxmemory() + extra > maxmemoryBytes) {
            throw new YierdisDb.YierdisCommandException(YierdisDb.OOM_ERR);
        }
    }

    void prepareWrite(long estimatedExtraBytes) {
        if (maxmemoryBytes <= 0) {
            return;
        }

        // Best-effort: try to reclaim expired keys first (Redis does this too under pressure).
        cleanupExpiredAll();

        long extra = Math.max(0, estimatedExtraBytes);
        if (extra > 0 && extra > maxmemoryBytes) {
            throw new YierdisDb.YierdisCommandException(YierdisDb.OOM_ERR);
        }
        long limit = maxmemoryBytes - extra;
        if (limit < 0) {
            limit = 0;
        }
        if (globalUsedBytesForMaxmemory() <= limit) {
            return;
        }

        if (policy == YierdisDb.MaxmemoryPolicy.NOEVICTION) {
            if (extra > 0) {
                throw new YierdisDb.YierdisCommandException(YierdisDb.OOM_ERR);
            }
            // extra == 0: allow "no growth" operations even when already above maxmemory.
            return;
        }

        evictUntilUnder(limit);
        if (globalUsedBytesForMaxmemory() > limit) {
            if (extra > 0) {
                throw new YierdisDb.YierdisCommandException(YierdisDb.OOM_ERR);
            }
        }
    }

    void enforceMaxmemory() {
        if (maxmemoryBytes <= 0) {
            return;
        }

        // Best-effort: try to reclaim expired keys first (Redis does this too under pressure).
        cleanupExpiredAll();

        if (globalUsedBytesForMaxmemory() <= maxmemoryBytes) {
            return;
        }

        if (policy == YierdisDb.MaxmemoryPolicy.NOEVICTION) {
            throw new YierdisDb.YierdisCommandException(YierdisDb.OOM_ERR);
        }

        evictUntilUnder(maxmemoryBytes);
        if (globalUsedBytesForMaxmemory() > maxmemoryBytes) {
            throw new YierdisDb.YierdisCommandException(YierdisDb.OOM_ERR);
        }
    }

    private void cleanupExpiredAll() {
        for (int i = 0; i < dbs.length; i++) {
            YierdisDb db = dbs[i];
            if (db == null) {
                continue;
            }
            db.cleanupExpired();
        }
    }

    private long globalUsedBytesForMaxmemory() {
        long heap = 0;
        for (int i = 0; i < dbs.length; i++) {
            YierdisDb db = dbs[i];
            if (db == null) {
                continue;
            }
            heap += Math.max(0L, db.usedBytes);
        }

        long offHeap = 0;
        if (offHeapAllocator != null) {
            try {
                offHeap = Math.max(0L, offHeapAllocator.usedBytes());
            } catch (Throwable ignored) {
                offHeap = 0;
            }
        }
        return heap + offHeap;
    }

    private int globalKeyCount() {
        int total = 0;
        for (int i = 0; i < dbs.length; i++) {
            YierdisDb db = dbs[i];
            if (db == null) {
                continue;
            }
            int size;
            try {
                size = db.store.size();
            } catch (Throwable ignored) {
                size = 0;
            }
            if (size <= 0) {
                continue;
            }
            if (Integer.MAX_VALUE - total < size) {
                return Integer.MAX_VALUE;
            }
            total += size;
        }
        return total;
    }

    private void evictUntilUnder(long limitBytes) {
        if (limitBytes < 0) {
            limitBytes = 0;
        }
        if (globalUsedBytesForMaxmemory() <= limitBytes) {
            return;
        }

        int totalKeys = globalKeyCount();
        int maxAttempts = Math.max(64, totalKeys * 2);

        long nowMillis = System.currentTimeMillis();
        long deadline = System.nanoTime() + evictionTimeLimitNanos;
        int attempts = 0;
        while (globalUsedBytesForMaxmemory() > limitBytes && attempts++ < maxAttempts) {
            if (evictionTimeLimitNanos > 0 && System.nanoTime() >= deadline) {
                break;
            }
            Candidate victim = pickVictim(nowMillis, totalKeys);
            if (victim == null) {
                break;
            }
            evictCandidate(victim, nowMillis);
        }
    }

    private Candidate pickVictim(long nowMillis, int totalKeys) {
        if (policy == YierdisDb.MaxmemoryPolicy.ALLKEYS_RANDOM) {
            return sampleAnyKey(nowMillis);
        }
        if (policy != YierdisDb.MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }

        // 如果 samples >= 全局 key 数量，则使用确定性全扫描（减少测试抖动）。
        if (totalKeys > 0 && samples >= totalKeys) {
            final Candidate[] bestRef = new Candidate[1];
            for (int i = 0; i < dbs.length; i++) {
                YierdisDb db = dbs[i];
                if (db == null) {
                    continue;
                }
                db.store.forEach((k, e) -> {
                    if (k == null || e == null) {
                        return;
                    }
                    if (db.isKeyExpired(k, nowMillis)) {
                        return;
                    }
                    long lru = e.lruClock;
                    Candidate best = bestRef[0];
                    if (best == null || lru < best.lruClock) {
                        bestRef[0] = new Candidate(db, k, lru);
                    }
                });
            }
            return bestRef[0];
        }

        Candidate best = null;
        for (int i = 0; i < samples; i++) {
            Candidate c = sampleAnyKey(nowMillis);
            if (c == null) {
                continue;
            }
            if (best == null || c.lruClock < best.lruClock) {
                best = c;
            }
        }
        return best;
    }

    private Candidate sampleAnyKey(long nowMillis) {
        if (dbs.length == 0) {
            return null;
        }

        int start = java.util.concurrent.ThreadLocalRandom.current().nextInt(dbs.length);
        for (int i = 0; i < dbs.length; i++) {
            YierdisDb db = dbs[(start + i) % dbs.length];
            if (db == null || db.store.size() == 0) {
                continue;
            }
            byte[] k = db.store.randomKey();
            if (k == null) {
                continue;
            }
            YierdisObject e = db.store.get(k);
            if (e == null) {
                continue;
            }
            if (db.isKeyExpired(k, nowMillis)) {
                continue;
            }
            return new Candidate(db, k, e.lruClock);
        }
        return null;
    }

    private void evictCandidate(Candidate victim, long nowMillis) {
        if (victim == null || victim.db == null || victim.key == null) {
            return;
        }
        YierdisDb db = victim.db;
        byte[] key = victim.key;
        YierdisObject e = db.store.get(key);
        if (e == null) {
            return;
        }
        if (db.removeIfExpired(key, e, nowMillis)) {
            return;
        }
        db.removeExpire(key);
        if (db.store.remove(key, e)) {
            e.releasePayloadIfAny();
            db.adjustUsedBytes(-e.estimatedBytes);
        }
    }

    private static final class Candidate {
        final YierdisDb db;
        final byte[] key;
        final long lruClock;

        private Candidate(YierdisDb db, byte[] key, long lruClock) {
            this.db = db;
            this.key = key;
            this.lruClock = lruClock;
        }
    }
}
