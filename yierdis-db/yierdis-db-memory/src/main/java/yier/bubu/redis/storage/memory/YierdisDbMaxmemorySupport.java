package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Objects;

public final class YierdisDbMaxmemorySupport {
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
        int maxAttempts = Math.max(64, db.keyLifecycle().keyCount() * 2);
        long nowMillis = System.currentTimeMillis();
        long deadline = System.nanoTime() + evictionTimeLimitNanos;
        while (db.usedBytesForMaxmemory() > limitBytes && attempts++ < maxAttempts) {
            if (System.nanoTime() >= deadline) {
                break;
            }
            KeyHandle victim = pickEvictionKey(nowMillis);
            if (victim == null) {
                break;
            }
            YierdisObject e = db.keyLifecycle().getStoredObject(victim);
            if (e == null) {
                continue;
            }
            if (db.removeIfExpired(victim, e, nowMillis)) {
                continue;
            }
            long removalBytes = db.keyLifecycle().estimatedBytesForRemoval(victim, e);
            db.removeExpire(victim);
            if (db.keyLifecycle().removeObject(victim, e)) {
                e.releasePayloadIfAny();
                db.adjustUsedBytes(-removalBytes);
            }
        }
    }

    MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
        if (policy == null || policy == MaxmemoryPolicy.NOEVICTION) {
            return null;
        }
        if (db.keyLifecycle().keyCount() == 0) {
            return null;
        }

        KeyHandle keyHandle = db.keyLifecycle().randomKeyHandle();
        if (keyHandle == null) {
            return null;
        }
        YierdisObject e = db.keyLifecycle().getStoredObject(keyHandle);
        if (e == null) {
            return null;
        }
        if (db.isKeyExpired(keyHandle, nowMillis)) {
            return null;
        }

        long lruClock = policy == MaxmemoryPolicy.ALLKEYS_LRU ? e.lruClock : 0L;
        return new MaxmemoryCandidate(db, keyHandle, lruClock);
    }

    MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
        if (policy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }
        if (db.keyLifecycle().keyCount() == 0) {
            return null;
        }

        final KeyHandle[] bestKeyHandleRef = new KeyHandle[1];
        final long[] bestLruRef = new long[]{Long.MAX_VALUE};
        db.keyLifecycle().forEachKeyHandle((k, e) -> {
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
        return new MaxmemoryCandidate(db, bestKeyHandle, bestLruRef[0]);
    }

    boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
        if (candidate == null || candidate.owner() != db) {
            return false;
        }

        if (!(candidate.keyHandle() instanceof KeyHandle key)) {
            return false;
        }
        YierdisObject e = db.keyLifecycle().getStoredObject(key);
        if (e == null) {
            return false;
        }
        if (db.removeIfExpired(key, e, nowMillis)) {
            return true;
        }
        long removalBytes = db.keyLifecycle().estimatedBytesForRemoval(key, e);
        db.removeExpire(key);
        if (db.keyLifecycle().removeObject(key, e)) {
            e.releasePayloadIfAny();
            db.adjustUsedBytes(-removalBytes);
            return true;
        }
        return false;
    }

    private KeyHandle pickEvictionKey(long nowMillis) {
        if (db.keyLifecycle().keyCount() == 0) {
            return null;
        }

        if (maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_RANDOM) {
            return db.keyLifecycle().randomKeyHandle();
        }

        if (maxmemoryPolicy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }

        int total = db.keyLifecycle().keyCount();
        KeyHandle bestKey = null;
        long bestLru = Long.MAX_VALUE;
        int samples = Math.max(1, maxmemorySamples);

        if (samples >= total) {
            final KeyHandle[] bestKeyRef = new KeyHandle[1];
            final long[] bestLruRef = new long[]{Long.MAX_VALUE};
            db.keyLifecycle().forEachKeyHandle((k, e) -> {
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
            KeyHandle key = db.keyLifecycle().randomKeyHandle();
            if (key == null) {
                break;
            }
            YierdisObject e = db.keyLifecycle().getStoredObject(key);
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
