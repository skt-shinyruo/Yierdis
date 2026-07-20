package yier.bubu.redis.storage.memory;

import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.expire.YierdisDbExpirationSupport;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedCallbackMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

import java.util.Objects;

final class YierdisDbDataMaintenance {
    private final YierdisDbRuntimeState runtimeState;
    private final YierdisDbHealth health;
    private final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbExpirationSupport expirationSupport;
    private final YierdisDbMaxmemorySupport maxmemorySupport;
    private final YierdisDbMemoryReporter memoryReporter;

    YierdisDbDataMaintenance(
            YierdisDbRuntimeState runtimeState,
            YierdisDbHealth health,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbExpirationSupport expirationSupport,
            YierdisDbMaxmemorySupport maxmemorySupport,
            YierdisDbMemoryReporter memoryReporter
    ) {
        this.runtimeState = Objects.requireNonNull(runtimeState, "runtimeState");
        this.health = Objects.requireNonNull(health, "health");
        this.hashTableMaintenanceRegistry = Objects.requireNonNull(
                hashTableMaintenanceRegistry,
                "hashTableMaintenanceRegistry"
        );
        this.mutationExecutor = Objects.requireNonNull(mutationExecutor, "mutationExecutor");
        this.expirationSupport = Objects.requireNonNull(expirationSupport, "expirationSupport");
        this.maxmemorySupport = Objects.requireNonNull(maxmemorySupport, "maxmemorySupport");
        this.memoryReporter = Objects.requireNonNull(memoryReporter, "memoryReporter");
    }

    void enforceMaxmemory() {
        health.requireWritable();
        runtimeState.enforceMaxmemory();
    }

    void defragMaintenance() {
        runtimeState.defragMaintenance();
    }

    HashTableMaintenanceResult rehashMaintenance(HashTableWorkBudget budget) {
        runtimeState.checkThread();
        health.requireWritable();
        return hashTableMaintenanceRegistry.advance(
                Objects.requireNonNull(budget, "budget"),
                this::prepareMaintenanceResize
        );
    }

    private HashTableMaintenanceRegistry.PreparationResult prepareMaintenanceResize(
            HashTableMaintenanceRegistry.Participant participant
    ) {
        try {
            mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Void>() {
                @Override
                public long upperBoundBytes() {
                    return maintenanceUpperBoundBytes(participant);
                }

                @Override
                public boolean requiresCommitStream() {
                    return false;
                }

                @Override
                public yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation<Void> prepare() {
                    HashTableMaintenanceRegistry.MaintenancePreparation preparation = participant.prepareMaintenance();
                    if (preparation == null) {
                        return new PreparedCallbackMutation<>(
                                null,
                                0L,
                                0L,
                                MutationOutcome.NONE,
                                () -> {
                                },
                                null,
                                null
                        );
                    }
                    return new PreparedCallbackMutation<>(
                            null,
                            0L,
                            preparation.stagedNonNativeGrowthBytes(),
                            MutationOutcome.NONE,
                            preparation::commit,
                            null,
                            preparation::abort
                    );
                }
            });
        } catch (yier.bubu.redis.storage.api.YierdisCommandException expected) {
            return HashTableMaintenanceRegistry.PreparationResult.CAPACITY_LIMIT;
        }
        return participant.hasMaintenanceDebt()
                ? HashTableMaintenanceRegistry.PreparationResult.STARTED
                : HashTableMaintenanceRegistry.PreparationResult.NO_CHANGE;
    }

    private long maintenanceUpperBoundBytes(HashTableMaintenanceRegistry.Participant participant) {
        long stagedGrowth = Math.max(0L, participant.estimatedMaintenanceGrowthBytes());
        long scopeBookkeeping = MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes(
                runtimeState.nativeAllocator(),
                0
        );
        return stagedGrowth > Long.MAX_VALUE - scopeBookkeeping
                ? Long.MAX_VALUE
                : stagedGrowth + scopeBookkeeping;
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
        health.requireWritable();
        expirationSupport.cleanupExpired(nowMillis);
    }

    void cleanupExpired() {
        runtimeState.checkThread();
        health.requireWritable();
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
        health.requireWritable();
        if (candidate == null || candidate.owner() != publicOwner) {
            return false;
        }
        return maxmemorySupport.evict(internalCandidate(candidate), nowMillis);
    }

    void shutdown() {
        runtimeState.shutdown();
    }

    MutationOutcome flushDb(MutationContext context) {
        health.requireWritable();
        return mutationExecutor.execute(
                Objects.requireNonNull(context, "context"),
                new YierdisDbMutationExecutor.MutationPlan<>() {
                    @Override
                    public long upperBoundBytes() {
                        return 0L;
                    }

                    @Override
                    public AdmissionMode admissionMode() {
                        return AdmissionMode.RECLAMATION;
                    }

                    @Override
                    public yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation<MutationOutcome> prepare() {
                        YierdisDbRuntimeState.FlushPreparation preparation = runtimeState.prepareFlushDb();
                        return new PreparedCallbackMutation<>(
                                preparation.outcome(),
                                preparation.committedMemoryDelta(),
                                0L,
                                preparation.outcome(),
                                runtimeState::commitFlushDb,
                                null,
                                null
                        );
                    }
                }
        );
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
