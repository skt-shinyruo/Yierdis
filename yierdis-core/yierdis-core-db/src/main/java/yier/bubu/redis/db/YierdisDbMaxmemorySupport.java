package yier.bubu.redis.db;

import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.ops.MaxmemoryCandidate;
import yier.bubu.redis.ops.MaxmemoryPolicy;

import java.util.Objects;

final class YierdisDbMaxmemorySupport {
    private final YierdisDb db;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitNanos;

    YierdisDbMaxmemorySupport(
            YierdisDb db,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitNanos
    ) {
        this.db = Objects.requireNonNull(db, "db");
        this.maxmemoryPolicy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        this.maxmemorySamples = maxmemorySamples;
        this.evictionTimeLimitNanos = evictionTimeLimitNanos;
    }

    void evictUntilUnder(long limitBytes) {
        if (limitBytes < 0) {
            limitBytes = 0;
        }
        if (db.usedBytesForMaxmemory() <= limitBytes) {
            return;
        }

        int attempts = 0;
        int maxAttempts = Math.max(64, db.store.size() * 2);
        long nowMillis = System.currentTimeMillis();
        long deadline = System.nanoTime() + evictionTimeLimitNanos;
        while (db.usedBytesForMaxmemory() > limitBytes && attempts++ < maxAttempts) {
            if (System.nanoTime() >= deadline) {
                break;
            }
            byte[] victim = pickEvictionKey(nowMillis);
            if (victim == null) {
                break;
            }
            YierdisObject e = db.store.get(victim);
            if (e == null) {
                continue;
            }
            if (db.removeIfExpired(victim, e, nowMillis)) {
                continue;
            }
            db.removeExpire(victim);
            if (db.store.remove(victim, e)) {
                e.releasePayloadIfAny();
                db.adjustUsedBytes(-e.estimatedBytes);
            }
        }
    }

    MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
        if (policy == null || policy == MaxmemoryPolicy.NOEVICTION) {
            return null;
        }
        if (db.store.size() == 0) {
            return null;
        }

        byte[] key = db.store.randomKey();
        if (key == null) {
            return null;
        }
        YierdisObject e = db.store.get(key);
        if (e == null) {
            return null;
        }
        if (db.isKeyExpired(key, nowMillis)) {
            return null;
        }

        long lruClock = policy == MaxmemoryPolicy.ALLKEYS_LRU ? e.lruClock : 0L;
        return new MaxmemoryCandidate(db, key, lruClock);
    }

    MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
        if (policy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }
        if (db.store.size() == 0) {
            return null;
        }

        final KeyHandle[] bestKeyHandleRef = new KeyHandle[1];
        final long[] bestLruRef = new long[]{Long.MAX_VALUE};
        db.store.forEachKeyHandle((k, e) -> {
            if (k == null || e == null) {
                return;
            }
            if (db.isKeyExpired(k, nowMillis)) {
                return;
            }
            long lru = e.lruClock;
            if (bestKeyHandleRef[0] == null || lru < bestLruRef[0]) {
                bestKeyHandleRef[0] = k;
                bestLruRef[0] = lru;
            }
        });

        KeyHandle bestKeyHandle = bestKeyHandleRef[0];
        if (bestKeyHandle == null) {
            return null;
        }
        byte[] keyBytes = YierdisDb.toByteArray(bestKeyHandle);
        if (keyBytes == null) {
            return null;
        }
        return new MaxmemoryCandidate(db, keyBytes, bestLruRef[0]);
    }

    boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
        if (candidate == null || candidate.owner() != db) {
            return false;
        }

        byte[] key = candidate.key();
        YierdisObject e = db.store.get(key);
        if (e == null) {
            return false;
        }
        if (db.removeIfExpired(key, e, nowMillis)) {
            return true;
        }
        db.removeExpire(key);
        if (db.store.remove(key, e)) {
            e.releasePayloadIfAny();
            db.adjustUsedBytes(-e.estimatedBytes);
            return true;
        }
        return false;
    }

    private byte[] pickEvictionKey(long nowMillis) {
        if (db.store.size() == 0) {
            return null;
        }

        if (maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_RANDOM) {
            return db.store.randomKey();
        }

        if (maxmemoryPolicy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }

        int total = db.store.size();
        byte[] bestKey = null;
        long bestLru = Long.MAX_VALUE;
        int samples = Math.max(1, maxmemorySamples);

        if (samples >= total) {
            final byte[][] bestKeyRef = new byte[1][];
            final long[] bestLruRef = new long[]{Long.MAX_VALUE};
            db.store.forEach((k, e) -> {
                if (db.isKeyExpired(k, nowMillis)) {
                    return;
                }
                long lru = e.lruClock;
                if (bestKeyRef[0] == null || lru < bestLruRef[0]) {
                    bestKeyRef[0] = k;
                    bestLruRef[0] = lru;
                }
            });
            return bestKeyRef[0];
        }

        for (int i = 0; i < samples; i++) {
            byte[] key = db.store.randomKey();
            if (key == null) {
                break;
            }
            YierdisObject e = db.store.get(key);
            if (e == null) {
                continue;
            }
            if (db.isKeyExpired(key, nowMillis)) {
                continue;
            }
            long lru = e.lruClock;
            if (bestKey == null || lru < bestLru) {
                bestKey = key;
                bestLru = lru;
            }
        }
        return bestKey;
    }
}
