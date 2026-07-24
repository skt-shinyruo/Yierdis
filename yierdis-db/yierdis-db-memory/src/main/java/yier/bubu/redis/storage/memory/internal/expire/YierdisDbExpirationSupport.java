package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.YierdisDbInternals;

import java.util.Objects;

public final class YierdisDbExpirationSupport {
    private static final int CLEANUP_SAMPLES_PER_LOOP = 20;
    private static final int CLEANUP_MAX_LOOPS = 16;

    private final Runnable threadChecker;
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final long expireCleanupTimeLimitNanos;

    public YierdisDbExpirationSupport(
            Runnable threadChecker,
            YierdisDbInternals internals,
            YierdisDbKeyLifecycle keyLifecycle,
            long expireCleanupTimeLimitNanos
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.expireCleanupTimeLimitNanos = expireCleanupTimeLimitNanos;
    }

    public void cleanupExpired() {
        cleanupExpired(0L);
    }

    public void cleanupExpired(long nowMillis) {
        threadChecker.run();
        long deadlineNanos = System.nanoTime() + expireCleanupTimeLimitNanos;
        long nowFixed = nowMillis <= 0 ? System.currentTimeMillis() : nowMillis;
        int loops = 0;

        for (; ; ) {
            int total = keyLifecycle.expireCount();
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
            KeyHandle keyHandle = keyLifecycle.randomExpireKeyHandle();
            if (keyHandle == null) {
                break;
            }

            Long expireAtMillis = keyLifecycle.expireAtMillis(keyHandle);
            if (expireAtMillis == null) {
                keyLifecycle.removeExpire(keyHandle);
                continue;
            }

            EntryRecord record = keyLifecycle.entryRecord(keyHandle);
            if (record == null) {
                keyLifecycle.removeExpire(keyHandle);
                continue;
            }

            if (expireAtMillis <= nowMillis && internals.reclaimExpired(keyHandle, record, nowMillis)) {
                expired++;
            }
        }
        return expired;
    }
}
