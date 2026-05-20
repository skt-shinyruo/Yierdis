package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.expire.YierdisDbExpirationSupport;

import java.util.Objects;

final class YierdisDbDataMaintenance {
    private final YierdisDbRuntimeState runtimeState;
    private final YierdisDbExpirationSupport expirationSupport;
    private final YierdisDbMaxmemorySupport maxmemorySupport;
    private final YierdisDbMemoryReporter memoryReporter;

    YierdisDbDataMaintenance(
            YierdisDbRuntimeState runtimeState,
            YierdisDbExpirationSupport expirationSupport,
            YierdisDbMaxmemorySupport maxmemorySupport,
            YierdisDbMemoryReporter memoryReporter
    ) {
        this.runtimeState = Objects.requireNonNull(runtimeState, "runtimeState");
        this.expirationSupport = Objects.requireNonNull(expirationSupport, "expirationSupport");
        this.maxmemorySupport = Objects.requireNonNull(maxmemorySupport, "maxmemorySupport");
        this.memoryReporter = Objects.requireNonNull(memoryReporter, "memoryReporter");
    }

    void enforceMaxmemory() {
        runtimeState.enforceMaxmemory();
    }

    void defragMaintenance() {
        runtimeState.defragMaintenance();
    }

    long usedBytesForMaxmemory() {
        return memoryReporter.usedBytesForMaxmemory();
    }

    int keyCountEstimate() {
        return memoryReporter.keyCountEstimate();
    }

    long estimatedUsedBytes() {
        return memoryReporter.estimatedUsedBytes();
    }

    void cleanupExpired(long nowMillis) {
        runtimeState.checkThread();
        expirationSupport.cleanupExpired(nowMillis);
    }

    void cleanupExpired() {
        runtimeState.checkThread();
        expirationSupport.cleanupExpired();
    }

    MaxmemoryCandidate sampleCandidate(MaxmemoryParticipant publicOwner, MaxmemoryPolicy policy, long nowMillis) {
        runtimeState.checkThread();
        return publicCandidate(publicOwner, maxmemorySupport.sampleCandidate(policy, nowMillis));
    }

    MaxmemoryCandidate scanBestCandidate(MaxmemoryParticipant publicOwner, MaxmemoryPolicy policy, long nowMillis) {
        runtimeState.checkThread();
        return publicCandidate(publicOwner, maxmemorySupport.scanBestCandidate(policy, nowMillis));
    }

    boolean evict(MaxmemoryParticipant publicOwner, MaxmemoryCandidate candidate, long nowMillis) {
        runtimeState.checkThread();
        if (candidate == null || candidate.owner() != publicOwner) {
            return false;
        }
        return maxmemorySupport.evict(internalCandidate(candidate), nowMillis);
    }

    void shutdown() {
        runtimeState.shutdown();
    }

    MutationOutcome flushDb() {
        return runtimeState.flushDb();
    }

    int size() {
        return runtimeState.size();
    }

    private MaxmemoryCandidate publicCandidate(MaxmemoryParticipant publicOwner, MaxmemoryCandidate candidate) {
        Objects.requireNonNull(publicOwner, "publicOwner");
        if (candidate == null) {
            return null;
        }
        return new MaxmemoryCandidate(publicOwner, candidate.keyHandle(), candidate.lruClock());
    }

    private MaxmemoryCandidate internalCandidate(MaxmemoryCandidate candidate) {
        return new MaxmemoryCandidate(maxmemorySupport, candidate.keyHandle(), candidate.lruClock());
    }
}
