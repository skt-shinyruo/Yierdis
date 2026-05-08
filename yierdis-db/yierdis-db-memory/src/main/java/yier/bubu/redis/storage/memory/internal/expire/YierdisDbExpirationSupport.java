package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

import java.util.Objects;

public final class YierdisDbExpirationSupport {
    private static final int CLEANUP_SAMPLES_PER_LOOP = 20;
    private static final int CLEANUP_MAX_LOOPS = 16;

    private final YierdisDb db;
    private final long expireCleanupTimeLimitNanos;

    public YierdisDbExpirationSupport(YierdisDb db, long expireCleanupTimeLimitNanos) {
        this.db = Objects.requireNonNull(db, "db");
        this.expireCleanupTimeLimitNanos = expireCleanupTimeLimitNanos;
    }

    public void cleanupExpired() {
        cleanupExpired(0L);
    }

    public void cleanupExpired(long nowMillis) {
        db.checkThread();
        long deadlineNanos = System.nanoTime() + expireCleanupTimeLimitNanos;
        long nowFixed = nowMillis <= 0 ? System.currentTimeMillis() : nowMillis;
        int loops = 0;

        for (; ; ) {
            int total = db.keyLifecycle().expireCount();
            if (total == 0) {
                return;
            }

            int samples = Math.min(CLEANUP_SAMPLES_PER_LOOP, total);
            if (samples <= 0) {
                return;
            }

            int expired = cleanupSamples(samples, nowFixed);

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

    private int cleanupSamples(int samples, long nowMillis) {
        int expired = 0;
        for (int i = 0; i < samples; i++) {
            KeyHandle keyHandle = db.keyLifecycle().randomExpireKeyHandle();
            if (keyHandle == null) {
                break;
            }

            Long expireAtMillis = db.keyLifecycle().expireAtMillis(keyHandle);
            if (expireAtMillis == null) {
                db.removeExpire(keyHandle);
                continue;
            }

            YierdisObject e = db.keyLifecycle().getStoredObject(keyHandle);
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

    private void removeExpiredValue(KeyHandle keyHandle, YierdisObject e) {
        db.removeExpire(keyHandle);
        if (db.keyLifecycle().removeObject(keyHandle, e)) {
            e.releasePayloadIfAny();
            db.adjustUsedBytes(-e.estimatedBytes);
        }
    }
}
