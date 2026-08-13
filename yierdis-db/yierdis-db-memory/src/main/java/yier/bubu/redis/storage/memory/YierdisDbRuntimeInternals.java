package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;
import yier.bubu.redis.common.command.ByteArrayCommandRecord;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.value.NativeBytesSlice;
import yier.bubu.redis.storage.memory.internal.value.NativeListEntryRef;
import yier.bubu.redis.storage.memory.internal.value.PinnedPoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.value.PreparedPoppedValueSequence;

import java.util.Objects;

public final class YierdisDbRuntimeInternals {
    private static final byte[] DELETE_COMMAND = new byte[]{'D', 'E', 'L'};
    private final Runnable threadChecker;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final MemoryLedger ledger;
    private final StableMemoryBackend stableMemoryBackend;

    YierdisDbRuntimeInternals(
            Runnable threadChecker,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbKeyLifecycle keyLifecycle,
            MemoryLedger ledger,
            StableMemoryBackend stableMemoryBackend
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.mutationExecutor = Objects.requireNonNull(mutationExecutor, "mutationExecutor");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.stableMemoryBackend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
    }

    public void checkThread() {
        threadChecker.run();
    }

    public <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan) {
        return executeMutation(MutationContext.none(), plan);
    }

    public <T> T executeMutation(
            MutationContext context,
            YierdisDbMutationExecutor.MutationPlan<T> plan
    ) {
        return mutationExecutor.execute(
                Objects.requireNonNull(context, "context"),
                Objects.requireNonNull(plan, "plan")
        );
    }

    public EntryRecord liveEntryRecord(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return null;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        long nowMillis = System.currentTimeMillis();
        if (!keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return record;
        }
        reclaimExpired(keyHandle, record, nowMillis);
        return null;
    }

    public boolean reclaimExpired(KeyHandle keyHandle, EntryRecord expectedRecord, long nowMillis) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        ReclamationAttempt attempt = new ReclamationAttempt();
        if (!keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return false;
        }
        try {
            return reclaim(keyHandle, expectedRecord, DbCommitKind.EXPIRED, nowMillis, true, attempt);
        } catch (DbCommitStreamUnavailableException unavailable) {
            if (attempt.entryHandle == null) {
                attempt.track(
                        keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(keyHandle)),
                        expectedRecord
                );
            }
            keyLifecycle.markExpiredEntryAwaitingPhysicalDeletion(
                    keyHandle,
                    attempt.entryHandle,
                    attempt.record,
                    nowMillis
            );
            return false;
        }
    }

    public boolean evict(KeyHandle keyHandle, EntryRecord expectedRecord) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return reclaim(keyHandle, expectedRecord, DbCommitKind.EVICTED, 0L, false, null);
    }

    public YierdisDbKeyLifecycle keyLifecycle() {
        return keyLifecycle;
    }

    public MemoryLedger ledger() {
        return ledger;
    }

    public long nativeAllocationPeakAdditionalBytes(
            long ffmRegionGrowthBytes,
            long heapGrowthBytes,
            int... nativeAllocationSizes
    ) {
        return MutationMemoryEstimator.peakAdditionalBytes(
                stableMemoryBackend,
                ffmRegionGrowthBytes,
                heapGrowthBytes,
                nativeAllocationSizes
        );
    }

    public long nativeAllocationScopeBookkeepingBytes(int expectedNativeAllocationCount) {
        return MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes(
                stableMemoryBackend,
                expectedNativeAllocationCount
        );
    }

    public NativeEpochScope beginScanEpoch() {
        return stableMemoryBackend.beginEpoch(NativeEpochKind.SCAN);
    }

    public NativeEpochScope beginSnapshotEpoch() {
        return stableMemoryBackend.beginEpoch(NativeEpochKind.SNAPSHOT);
    }

    public NativeBytesSlice keyBytesSlice(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return new NativeBytesSlice(
                stableMemoryBackend,
                KeyHandleAccess.allocatorNativeHandle(keyHandle),
                0,
                keyHandle.length()
        );
    }

    public PinnedPoppedValueSequence capturePoppedValues(NativeListEntryRef[] entries) {
        return PinnedPoppedValueSequence.capture(stableMemoryBackend, entries);
    }

    public PreparedPoppedValueSequence ownPoppedValues(NativeListEntryRef[] entries) {
        return PreparedPoppedValueSequence.owned(stableMemoryBackend, entries);
    }

    public MemoryReclaimResult trimEmptyNativePages(MemoryPressureBudget budget) {
        return stableMemoryBackend.trimEmptyPages(Objects.requireNonNull(budget, "budget"));
    }

    public MemoryUsageSnapshot nativeMemoryUsage() {
        return stableMemoryBackend.memoryUsage();
    }

    public NativeAllocatorStats nativeAllocatorStats() {
        return stableMemoryBackend.stats();
    }

    public long nativeLiveRegionCount() {
        return stableMemoryBackend.liveRegionCount();
    }

    public void trimEmptyNativePagesAfterPreparedPreviewClose() {
        if (ledger.maxmemoryEnabled()) {
            trimEmptyNativePages(MemoryPressureBudget.unlimited());
        }
    }

    private boolean reclaim(
            KeyHandle keyHandle,
            EntryRecord expectedRecord,
            DbCommitKind commitKind,
            long nowMillis,
            boolean requireExpired,
            ReclamationAttempt attempt
    ) {
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<>() {
            private byte[] deletedKey;

            @Override
            public long upperBoundBytes() {
                return 0L;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public DbCommitKind commitKind() {
                return commitKind;
            }

            @Override
            public ImmutableCommandRecord retainCommitRecord(MutationContext context) {
                if (deletedKey == null) {
                    throw new IllegalStateException("synthetic deletion record is unavailable");
                }
                return ByteArrayCommandRecord.copyOf(DELETE_COMMAND, deletedKey);
            }

            @Override
            public PreparedEntryMutation<Boolean> prepare() {
                EntryRecord current = keyLifecycle.entryRecord(keyHandle);
                if (current == null || (expectedRecord != null && !expectedRecord.equals(current))) {
                    return preparedNoDeletion();
                }
                if (requireExpired && !keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
                    return preparedNoDeletion();
                }
                byte[] keyBytes = keyLifecycle.copyKeyBytes(keyHandle);
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
                if (entryHandle == null) {
                    return preparedNoDeletion();
                }
                if (attempt != null) {
                    attempt.track(entryHandle, current);
                }
                long removalBytes = keyLifecycle.estimatedBytesForRemoval(keyHandle, current);
                deletedKey = keyBytes;
                return PreparedEntryMutation.delete(
                        keyLifecycle,
                        Boolean.TRUE,
                        -removalBytes,
                        MutationOutcome.VALUE_CHANGED,
                        entryHandle,
                        current,
                        true
                );
            }

            private PreparedEntryMutation<Boolean> preparedNoDeletion() {
                return PreparedEntryMutation.unchanged(
                        keyLifecycle,
                        Boolean.FALSE,
                        MutationOutcome.NONE
                );
            }
        });
    }

    private static final class ReclamationAttempt {
        private EntryHandle entryHandle;
        private EntryRecord record;

        private void track(EntryHandle entryHandle, EntryRecord record) {
            this.entryHandle = entryHandle;
            this.record = record;
        }
    }
}
