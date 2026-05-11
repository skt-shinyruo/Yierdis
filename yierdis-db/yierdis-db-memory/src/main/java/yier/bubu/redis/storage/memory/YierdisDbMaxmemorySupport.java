package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
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
            EntryRecord record = db.keyLifecycle().entryRecord(victim);
            if (record == null) {
                continue;
            }
            if (db.keyLifecycle().removeIfExpired(victim, record, nowMillis)) {
                continue;
            }
            removeRecord(victim, record);
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
        EntryRecord record = db.keyLifecycle().entryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        if (db.isKeyExpired(keyHandle, nowMillis)) {
            return null;
        }

        long lruClock = policy == MaxmemoryPolicy.ALLKEYS_LRU ? record.lruOrLfu() : 0L;
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
        db.keyLifecycle().forEachKeyHandle((k, record) -> {
            if (k == null || record == null) {
                return;
            }
            if (db.isKeyExpired(k, nowMillis)) {
                return;
            }
            long lru = record.lruOrLfu();
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
        EntryRecord record = db.keyLifecycle().entryRecord(key);
        if (record == null) {
            return false;
        }
        if (db.keyLifecycle().removeIfExpired(key, record, nowMillis)) {
            return true;
        }
        return removeRecord(key, record);
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
            db.keyLifecycle().forEachKeyHandle((k, record) -> {
                if (db.isKeyExpired(k, nowMillis)) {
                    return;
                }
                if (record == null) {
                    return;
                }
                long lru = record.lruOrLfu();
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
            EntryRecord record = db.keyLifecycle().entryRecord(key);
            if (record == null) {
                continue;
            }
            if (db.isKeyExpired(key, nowMillis)) {
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

    private boolean removeRecord(KeyHandle keyHandle, EntryRecord record) {
        long removalBytes = db.keyLifecycle().estimatedBytesForRemoval(keyHandle, record);
        byte[] keyBytes = copyKeyBytes(keyHandle);
        if (db.keyLifecycle().removeEntry(keyHandle, record)) {
            db.keyLifecycle().removeExpireByKeyBytes(keyBytes);
            db.adjustUsedBytes(-removalBytes);
            return true;
        }
        return false;
    }

    private static byte[] copyKeyBytes(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.len()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
    }
}
