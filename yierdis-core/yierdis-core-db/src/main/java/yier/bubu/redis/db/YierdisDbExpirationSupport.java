package yier.bubu.redis.db;

import yier.bubu.redis.db.key.KeyHandle;

import java.util.Objects;

final class YierdisDbExpirationSupport {
    private static final int CLEANUP_SAMPLES_PER_LOOP = 20;
    private static final int CLEANUP_MAX_LOOPS = 16;

    private final YierdisDb db;
    private final boolean keysStoredOffHeap;
    private final long expireCleanupTimeLimitNanos;

    YierdisDbExpirationSupport(YierdisDb db, boolean keysStoredOffHeap, long expireCleanupTimeLimitNanos) {
        this.db = Objects.requireNonNull(db, "db");
        this.keysStoredOffHeap = keysStoredOffHeap;
        this.expireCleanupTimeLimitNanos = expireCleanupTimeLimitNanos;
    }

    void cleanupExpired() {
        cleanupExpired(0L);
    }

    void cleanupExpired(long nowMillis) {
        db.checkThread();
        long deadlineNanos = System.nanoTime() + expireCleanupTimeLimitNanos;
        long nowFixed = nowMillis <= 0 ? System.currentTimeMillis() : nowMillis;
        int loops = 0;

        for (; ; ) {
            int total = db.expires.size();
            if (total == 0) {
                return;
            }

            int samples = Math.min(CLEANUP_SAMPLES_PER_LOOP, total);
            if (samples <= 0) {
                return;
            }

            int expired = keysStoredOffHeap
                    ? cleanupOffHeapSamples(samples, nowFixed)
                    : cleanupHeapSamples(samples, nowFixed);

            loops++;
            if (expired <= samples / 4) {
                return;
            }
            if (loops >= CLEANUP_MAX_LOOPS) {
                return;
            }
            if (System.nanoTime() >= deadlineNanos) {
                return;
            }
        }
    }

    private int cleanupOffHeapSamples(int samples, long nowMillis) {
        int expired = 0;
        for (int i = 0; i < samples; i++) {
            KeyHandle keyHandle = db.expires.randomKeyHandle();
            if (keyHandle == null) {
                break;
            }

            Long expireAtMillis = db.expires.get(keyHandle);
            if (expireAtMillis == null) {
                db.removeExpire(keyHandle);
                continue;
            }

            YierdisObject e = db.store.get(keyHandle);
            if (e == null) {
                db.removeExpire(keyHandle);
                continue;
            }

            if (expireAtMillis <= nowMillis) {
                removeExpiredValue(keyHandle, e);
                expired++;
            }
        }
        return expired;
    }

    private int cleanupHeapSamples(int samples, long nowMillis) {
        int expired = 0;
        for (int i = 0; i < samples; i++) {
            byte[] keyBytes = db.expires.randomKey();
            if (keyBytes == null) {
                break;
            }

            Long expireAtMillis = db.expires.get(keyBytes);
            if (expireAtMillis == null) {
                db.removeExpire(keyBytes);
                continue;
            }

            YierdisObject e = db.store.get(keyBytes);
            if (e == null) {
                db.removeExpire(keyBytes);
                continue;
            }

            if (expireAtMillis <= nowMillis) {
                removeExpiredValue(keyBytes, e);
                expired++;
            }
        }
        return expired;
    }

    private void removeExpiredValue(KeyHandle keyHandle, YierdisObject e) {
        db.removeExpire(keyHandle);
        if (db.store.remove(keyHandle, e)) {
            e.releasePayloadIfAny();
            db.adjustUsedBytes(-e.estimatedBytes);
        }
    }

    private void removeExpiredValue(byte[] keyBytes, YierdisObject e) {
        db.removeExpire(keyBytes);
        if (db.store.remove(keyBytes, e)) {
            e.releasePayloadIfAny();
            db.adjustUsedBytes(-e.estimatedBytes);
        }
    }
}
