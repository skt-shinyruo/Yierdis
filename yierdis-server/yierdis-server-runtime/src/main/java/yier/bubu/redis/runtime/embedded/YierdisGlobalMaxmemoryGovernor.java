package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SPI-driven global maxmemory governor (Redis-like best-effort semantics).
 * <p>
 * This governor depends only on {@code yierdis-db-api} maxmemory SPI and can coordinate across multiple
 * independent participants (e.g. multiple DBs). Each participant reports an owned physical snapshot.
 */
public final class YierdisGlobalMaxmemoryGovernor implements MaxmemoryCoordinator {
    private final GlobalMaxmemoryDbEngine[] participants;
    private final long maxmemoryBytes;
    private final MaxmemoryPolicy policy;
    private final int samples;
    private final long evictionTimeLimitNanos;
    private final AtomicLong globalLruClock = new AtomicLong(0);
    private int nextTrimParticipantIndex;

    public YierdisGlobalMaxmemoryGovernor(
            GlobalMaxmemoryDbEngine[] participants,
            long maxmemoryBytes,
            MaxmemoryPolicy policy,
            int samples,
            long evictionTimeLimitNanos
    ) {
        Objects.requireNonNull(participants, "participants");
        this.participants = participants.clone();
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
    public synchronized void enforceMaintenance() {
        prepareWrite(null, 0);
    }

    @Override
    public synchronized void prepareWrite(MaxmemoryParticipant requester, long estimatedExtraBytes) {
        if (estimatedExtraBytes < 0) {
            throw new IllegalArgumentException("estimatedExtraBytes < 0: " + estimatedExtraBytes);
        }
        if (maxmemoryBytes <= 0) {
            return;
        }

        long deadline = evictionDeadline(System.nanoTime());
        // Best-effort: try to reclaim expired keys first (Redis does this too under pressure).
        long nowMillis = System.currentTimeMillis();
        cleanupExpiredAll(nowMillis);
        trimAllParticipants(deadline);

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

        evictUntilUnder(limit, nowMillis, deadline);
        trimAllParticipants(deadline);
        if (globalUsedBytesForMaxmemory() > limit && extra > 0) {
            throw oom();
        }
    }

    private static YierdisCommandException oom() {
        return new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
    }

    private void cleanupExpiredAll(long nowMillis) {
        for (GlobalMaxmemoryDbEngine participant : participants) {
            if (participant == null) {
                continue;
            }
            participant.cleanupExpired(nowMillis);
        }
    }

    private void trimAllParticipants(long deadline) {
        if (participants.length == 0) {
            return;
        }
        int start = Math.floorMod(nextTrimParticipantIndex, participants.length);
        nextTrimParticipantIndex = (start + 1) % participants.length;
        for (int i = 0; i < participants.length; i++) {
            if (deadlineReached(deadline)) {
                return;
            }
            GlobalMaxmemoryDbEngine participant = participants[(start + i) % participants.length];
            if (participant == null) {
                continue;
            }
            participant.trimMemory(trimBudget(deadline));
        }
    }

    private long globalUsedBytesForMaxmemory() {
        long total = 0;

        for (GlobalMaxmemoryDbEngine participant : participants) {
            if (participant == null) {
                continue;
            }
            MemoryUsageSnapshot usage = participant.memoryUsage();
            long used = usage == null ? 0L : usage.effectiveBytesForMaxmemory();
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
        for (GlobalMaxmemoryDbEngine participant : participants) {
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

    private void evictUntilUnder(long limitBytes, long nowMillis, long deadline) {
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

        int attempts = 0;
        int stalledAttempts = 0;
        int maxStalledAttempts = Math.max(1, totalKeys);
        long currentUsed = globalUsedBytesForMaxmemory();
        while (currentUsed > limitBytes && attempts++ < maxAttempts) {
            if (deadlineReached(deadline)) {
                break;
            }

            MaxmemoryCandidate victim = pickVictim(nowMillis, totalKeys);
            if (victim == null) {
                break;
            }
            long beforeEvictionBytes = currentUsed;
            evictCandidate(victim, nowMillis);
            trimAllParticipants(deadline);
            currentUsed = globalUsedBytesForMaxmemory();
            if (currentUsed < beforeEvictionBytes) {
                stalledAttempts = 0;
                continue;
            }
            stalledAttempts++;
            if (stalledAttempts >= maxStalledAttempts) {
                break;
            }
        }
    }

    private long evictionDeadline(long startedNanos) {
        if (evictionTimeLimitNanos <= 0L) {
            return Long.MAX_VALUE;
        }
        if (startedNanos > Long.MAX_VALUE - evictionTimeLimitNanos) {
            return Long.MAX_VALUE;
        }
        return startedNanos + evictionTimeLimitNanos;
    }

    private boolean deadlineReached(long deadline) {
        return deadline != Long.MAX_VALUE && System.nanoTime() >= deadline;
    }

    private MemoryPressureBudget trimBudget(long deadline) {
        long timeLimitNanos = Long.MAX_VALUE;
        if (deadline != Long.MAX_VALUE) {
            timeLimitNanos = Math.max(0L, deadline - System.nanoTime());
        }
        return new MemoryPressureBudget(
                Math.max(16L, samples),
                Long.MAX_VALUE,
                timeLimitNanos
        );
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
        for (GlobalMaxmemoryDbEngine participant : participants) {
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
            GlobalMaxmemoryDbEngine participant = participants[(start + i) % participants.length];
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
