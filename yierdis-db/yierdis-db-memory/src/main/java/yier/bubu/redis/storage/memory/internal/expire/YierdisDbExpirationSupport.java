package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;

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

            EntryRecord record = db.keyLifecycle().entryRecord(keyHandle);
            if (record == null) {
                db.removeExpire(keyHandle);
                continue;
            }

            if (expireAtMillis <= nowMillis) {
                removeExpiredRecord(keyHandle, record);
                expired++;
            }
        }
        return expired;
    }

    private void removeExpiredRecord(KeyHandle keyHandle, EntryRecord record) {
        long removalBytes = db.keyLifecycle().estimatedBytesForRemoval(keyHandle, record);
        byte[] keyBytes = copyKeyBytes(keyHandle);
        if (db.keyLifecycle().removeEntry(keyHandle, record)) {
            db.keyLifecycle().removeExpireByKeyBytes(keyBytes);
            db.adjustUsedBytes(-removalBytes);
        }
    }

    private static byte[] copyKeyBytes(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.len()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
    }
}
