package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MaxmemoryUsageSource;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SPI-driven global maxmemory governor (Redis-like best-effort semantics).
 * <p>
 * This governor depends only on {@code yierdis-storage-api} maxmemory SPI and can coordinate across multiple
 * independent participants (e.g. multiple DBs) plus optional shared usage sources (e.g. a shared allocator)
 * that should be counted once.
 */
public final class YierdisGlobalMaxmemoryGovernor implements MaxmemoryCoordinator {
    private final MaxmemoryParticipant[] participants;
    private final MaxmemoryUsageSource[] sharedUsage;
    private final long maxmemoryBytes;
    private final MaxmemoryPolicy policy;
    private final int samples;
    private final long evictionTimeLimitNanos;
    private final AtomicLong globalLruClock = new AtomicLong(0);

    public YierdisGlobalMaxmemoryGovernor(
            MaxmemoryParticipant[] participants,
            MaxmemoryUsageSource[] sharedUsage,
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionTimeLimitNanos
    ) {
        Objects.requireNonNull(participants, "participants");
        this.participants = participants.clone();
        this.sharedUsage = sharedUsage == null ? new MaxmemoryUsageSource[0] : sharedUsage.clone();
        this.maxmemoryBytes = Math.max(0, maxmemoryBytes);
        this.policy = policy == null ? MaxmemoryPolicy.NOEVICTION : policy;
        this.samples = Math.max(1, samples);
        this.evictionTimeLimitNanos = Math.max(0, evictionTimeLimitNanos);
    }

    @Override
    public long nextLruClock() {
        return globalLruClock.incrementAndGet();
    }

    /**
     * Best-effort maintenance tick for global maxmemory without assuming write growth.
     */
    public void enforceMaintenance() {
        prepareWrite(0);
    }

    @Override
    public void prepareWrite(long estimatedExtraBytes) {
        if (estimatedExtraBytes < 0) {
            throw new IllegalArgumentException("estimatedExtraBytes < 0: " + estimatedExtraBytes);
        }
        if (maxmemoryBytes <= 0) {
            return;
        }

        // Best-effort: try to reclaim expired keys first (Redis does this too under pressure).
        long nowMillis = System.currentTimeMillis();
        cleanupExpiredAll(nowMillis);

        long extra = estimatedExtraBytes;
        if (extra > 0 && extra > maxmemoryBytes) {
            throw oom();
        }
        long limit = maxmemoryBytes - extra;
        if (limit < 0) {
            limit = 0;
        }
        if (globalUsedBytesForMaxmemory() <= limit) {
            return;
        }

        if (policy == MaxmemoryPolicy.NOEVICTION) {
            if (extra > 0) {
                throw oom();
            }
            // extra == 0: allow "no growth" operations even when already above maxmemory.
            return;
        }

        evictUntilUnder(limit, nowMillis);
        if (globalUsedBytesForMaxmemory() > limit && extra > 0) {
            throw oom();
        }
    }

    private static YierdisCommandException oom() {
        return new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
    }

    private void cleanupExpiredAll(long nowMillis) {
        for (MaxmemoryParticipant participant : participants) {
            if (participant == null) {
                continue;
            }
            participant.cleanupExpired(nowMillis);
        }
    }

    private long globalUsedBytesForMaxmemory() {
        long total = 0;

        for (MaxmemoryParticipant participant : participants) {
            if (participant == null) {
                continue;
            }
            long used = participant.usedBytesForMaxmemory();
            if (used <= 0) {
                continue;
            }
            if (Long.MAX_VALUE - total < used) {
                return Long.MAX_VALUE;
            }
            total += used;
        }

        for (MaxmemoryUsageSource source : sharedUsage) {
            if (source == null) {
                continue;
            }
            long used;
            try {
                used = source.usedBytes();
            } catch (Throwable ignored) {
                used = 0;
            }
            if (used <= 0) {
                continue;
            }
            if (Long.MAX_VALUE - total < used) {
                return Long.MAX_VALUE;
            }
            total += used;
        }

        return total;
    }

    private int globalKeyCountEstimate() {
        int total = 0;
        for (MaxmemoryParticipant participant : participants) {
            if (participant == null) {
                continue;
            }
            int c;
            try {
                c = participant.keyCountEstimate();
            } catch (Throwable ignored) {
                c = 0;
            }
            if (c <= 0) {
                continue;
            }
            if (Integer.MAX_VALUE - total < c) {
                return Integer.MAX_VALUE;
            }
            total += c;
        }
        return total;
    }

    private void evictUntilUnder(long limitBytes, long nowMillis) {
        if (limitBytes < 0) {
            limitBytes = 0;
        }
        if (globalUsedBytesForMaxmemory() <= limitBytes) {
            return;
        }

        int totalKeys = globalKeyCountEstimate();
        int maxAttemptsFromKeys;
        if (totalKeys > Integer.MAX_VALUE / 2) {
            maxAttemptsFromKeys = Integer.MAX_VALUE;
        } else {
            maxAttemptsFromKeys = totalKeys * 2;
        }
        int maxAttempts = Math.max(64, maxAttemptsFromKeys);

        long startNanos = System.nanoTime();
        long deadline;
        if (evictionTimeLimitNanos <= 0) {
            deadline = 0;
        } else if (startNanos > Long.MAX_VALUE - evictionTimeLimitNanos) {
            deadline = Long.MAX_VALUE;
        } else {
            deadline = startNanos + evictionTimeLimitNanos;
        }
        int attempts = 0;
        while (globalUsedBytesForMaxmemory() > limitBytes && attempts++ < maxAttempts) {
            if (evictionTimeLimitNanos > 0 && System.nanoTime() >= deadline) {
                break;
            }

            MaxmemoryCandidate victim = pickVictim(nowMillis, totalKeys);
            if (victim == null) {
                break;
            }
            evictCandidate(victim, nowMillis);
        }
    }

    private MaxmemoryCandidate pickVictim(long nowMillis, int totalKeys) {
        if (policy == MaxmemoryPolicy.ALLKEYS_RANDOM) {
            return sampleAnyCandidate(nowMillis);
        }
        if (policy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }

        // If samples cover all keys, try deterministic scan first to reduce test flakiness.
        if (totalKeys > 0 && samples >= totalKeys) {
            MaxmemoryCandidate best = scanBestCandidate(nowMillis);
            if (best != null) {
                return best;
            }
        }

        MaxmemoryCandidate best = null;
        for (int i = 0; i < samples; i++) {
            MaxmemoryCandidate c = sampleAnyCandidate(nowMillis);
            if (c == null) {
                continue;
            }
            if (best == null || c.lruClock() < best.lruClock()) {
                best = c;
            }
        }
        return best;
    }

    private MaxmemoryCandidate scanBestCandidate(long nowMillis) {
        MaxmemoryCandidate best = null;
        for (MaxmemoryParticipant participant : participants) {
            if (participant == null) {
                continue;
            }
            MaxmemoryCandidate c = participant.scanBestCandidate(policy, nowMillis);
            if (c == null) {
                continue;
            }
            if (best == null || c.lruClock() < best.lruClock()) {
                best = c;
            }
        }
        return best;
    }

    private MaxmemoryCandidate sampleAnyCandidate(long nowMillis) {
        if (participants.length == 0) {
            return null;
        }

        int start = ThreadLocalRandom.current().nextInt(participants.length);
        for (int i = 0; i < participants.length; i++) {
            MaxmemoryParticipant participant = participants[(start + i) % participants.length];
            if (participant == null) {
                continue;
            }
            MaxmemoryCandidate candidate = participant.sampleCandidate(policy, nowMillis);
            if (candidate != null) {
                return candidate;
            }
        }
        return null;
    }

    private static void evictCandidate(MaxmemoryCandidate victim, long nowMillis) {
        if (victim == null) {
            return;
        }
        MaxmemoryParticipant owner = victim.owner();
        if (owner == null) {
            return;
        }
        owner.evict(victim, nowMillis);
    }
}
