package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;

import java.util.Objects;

final class YierdisDbDataMaintenance {
    private static final long MAINTENANCE_REHASH_MAX_INSPECTED_SLOTS = 64L;
    private static final int ASYNC_FLUSH_RECLAIM_MAX_ENTRIES = 64;

    private final YierdisDbRuntimeState runtimeState;
    private final YierdisDbStorage storage;
    private final YierdisDbKernel kernel;
    private final YierdisDbMemoryContext memoryContext;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbMemoryLedger ledger;
    private final YierdisDbHealth health;
    private final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;
    private final YierdisDbExpirationSupport expirationSupport;
    private final YierdisDbMaxmemorySupport maxmemorySupport;
    private final YierdisDbMemoryReporter memoryReporter;
    private final long maintenanceTimeLimitNanos;

    YierdisDbDataMaintenance(
            YierdisDbRuntimeState runtimeState,
            YierdisDbStorage storage,
            YierdisDbKernel kernel,
            YierdisDbMemoryContext memoryContext,
            YierdisDbMemoryLedger ledger,
            YierdisDbHealth health,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
            YierdisDbExpirationSupport expirationSupport,
            YierdisDbMaxmemorySupport maxmemorySupport,
            YierdisDbMemoryReporter memoryReporter,
            long maintenanceTimeLimitNanos
    ) {
        this.runtimeState = Objects.requireNonNull(runtimeState, "runtimeState");
        this.storage = Objects.requireNonNull(storage, "storage");
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
        this.keyLifecycle = storage.keyLifecycle();
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.health = Objects.requireNonNull(health, "health");
        this.hashTableMaintenanceRegistry = Objects.requireNonNull(
                hashTableMaintenanceRegistry,
                "hashTableMaintenanceRegistry"
        );
        this.expirationSupport = Objects.requireNonNull(expirationSupport, "expirationSupport");
        this.maxmemorySupport = Objects.requireNonNull(maxmemorySupport, "maxmemorySupport");
        this.memoryReporter = Objects.requireNonNull(memoryReporter, "memoryReporter");
        if (maintenanceTimeLimitNanos < 0L) {
            throw new IllegalArgumentException("maintenanceTimeLimitNanos must be >= 0");
        }
        this.maintenanceTimeLimitNanos = maintenanceTimeLimitNanos;
    }

    void runMaintenance() {
        runtimeState.checkThread();
        reclaimDetachedEntries();
        health.requireWritable();
        expirationSupport.cleanupExpired();
        rehashMaintenance(HashTableWorkBudget.of(
                MAINTENANCE_REHASH_MAX_INSPECTED_SLOTS,
                maintenanceTimeLimitNanos
        ));
        enforceMaxmemory();
    }

    void runDeferredReclamation() {
        runtimeState.checkThread();
        reclaimDetachedEntries();
    }

    void enforceMaxmemory() {
        health.requireWritable();
        runtimeState.checkThread();
        ledger.enforceLocalMaintenance();
    }

    void defragMaintenance() {
        runtimeState.defragMaintenance(keyLifecycle::defragCycle);
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
            kernel.execute(new MutationUse<Void>() {
                @Override
                public long upperBoundBytes() {
                    return maintenanceUpperBoundBytes(participant);
                }

                @Override
                public PreparedDbMutation<Void> prepare(YierdisDbKernel scope) {
                    HashTableMaintenanceRegistry.MaintenancePreparation preparation = participant.prepareMaintenance();
                    if (preparation == null) {
                        return scope.callback(
                                null,
                                0L,
                                0L,
                                MutationOutcome.NONE,
                                () -> {
                                },
                                null,
                                null,
                                false
                        );
                    }
                    return scope.callback(
                            null,
                            0L,
                            preparation.stagedNonNativeGrowthBytes(),
                            MutationOutcome.NONE,
                            preparation::commit,
                            null,
                            preparation::abort,
                            false
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
        long scopeBookkeeping = memoryContext.nativeAllocationScopeBookkeepingBytes(0);
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
        if (!runtimeState.beginShutdown()) {
            return;
        }
        Throwable failure = null;
        try {
            ledger.resetUsage();
        } catch (Throwable next) {
            failure = next;
        }
        try {
            reclaimAllDetachedEntries();
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        try {
            kernel.close();
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        } finally {
            runtimeState.finishShutdown();
        }
        throwIfFailure(failure);
    }

    MutationOutcome flushDb() {
        return flushDb(false);
    }

    MutationOutcome flushDbAsync() {
        return flushDb(true);
    }

    private MutationOutcome flushDb(boolean async) {
        health.requireWritable();
        return kernel.execute(new MutationUse<MutationOutcome>() {
                    @Override
                    public long upperBoundBytes() {
                        return 0L;
                    }

                    @Override
                    public Admission admission() {
                        return Admission.RECLAMATION;
                    }

                    @Override
                    public PreparedDbMutation<MutationOutcome> prepare(YierdisDbKernel scope) {
                        FlushPreparation preparation = prepareFlushDb();
                        return scope.callback(
                                preparation.outcome(),
                                preparation.committedMemoryDelta(),
                                0L,
                                preparation.outcome(),
                                async
                                        ? YierdisDbDataMaintenance.this::commitFlushDbAsync
                                        : YierdisDbDataMaintenance.this::commitFlushDb,
                                null,
                                null,
                                !async && preparation.committedMemoryDelta() < 0L
                        );
                    }
                });
    }

    int size() {
        runtimeState.checkThread();
        return keyLifecycle.keyCount();
    }

    private void commitFlushDb() {
        runtimeState.checkThread();
        storage.clearData();
        keyLifecycle.resetExpireCount();
        expirationSupport.resetCursor();
    }

    private void commitFlushDbAsync() {
        // 只在 mutation commit 边界发布空目录；旧目录在后续 owner maintenance 中释放，不能再查询当前 keyspace。
        runtimeState.checkThread();
        storage.detachEntries();
        keyLifecycle.resetExpireCount();
        expirationSupport.resetCursor();
    }

    private void reclaimDetachedEntries() {
        try {
            int reclaimed = reclaimDetachedEntries(ASYNC_FLUSH_RECLAIM_MAX_ENTRIES);
            if (reclaimed > 0) {
                memoryContext.trimEmptyNativePages(MemoryPressureBudget.unlimited());
            }
        } catch (RuntimeException | Error failure) {
            health.recordInvariantFailure(failure);
            throw failure;
        }
    }

    private FlushPreparation prepareFlushDb() {
        runtimeState.checkThread();
        boolean hadKeys = keyLifecycle.keyCount() != 0;
        boolean hadTtl = keyLifecycle.expireCount() != 0;
        return new FlushPreparation(MutationOutcome.of(hadKeys, hadTtl), -ledger.usedBytes());
    }

    private int reclaimDetachedEntries(int maxEntries) {
        runtimeState.checkThread();
        return reclaimDetachedEntriesUnchecked(maxEntries);
    }

    private int reclaimDetachedEntriesUnchecked(int maxEntries) {
        if (maxEntries < 0) {
            throw new IllegalArgumentException("maxEntries must be >= 0");
        }
        Throwable failure = null;
        int attempted = 0;
        while (attempted < maxEntries && storage.detachedEntryCount() > 0) {
            try {
                storage.reclaimDetachedEntry();
            } catch (RuntimeException | Error next) {
                failure = recordFailure(failure, next);
            }
            attempted++;
        }
        throwIfFailure(failure);
        return attempted;
    }

    private void reclaimAllDetachedEntries() {
        Throwable failure = null;
        while (storage.detachedEntryCount() > 0) {
            try {
                // beginShutdown 已完成 owner 校验；CLOSING 状态下不能再走普通 DB access guard。
                reclaimDetachedEntriesUnchecked(Integer.MAX_VALUE);
            } catch (RuntimeException | Error next) {
                failure = recordFailure(failure, next);
            }
        }
        throwIfFailure(failure);
    }

    long detachedEntryCount() {
        runtimeState.checkThread();
        return storage.detachedEntryCount();
    }

    private static Throwable recordFailure(Throwable current, Throwable next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    private static void throwIfFailure(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("YierdisDb shutdown failed", failure);
    }

    private record FlushPreparation(MutationOutcome outcome, long committedMemoryDelta) {
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
