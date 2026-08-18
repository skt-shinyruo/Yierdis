package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.YierdisDbHealth;

public final class YierdisDbMutationExecutor {
    private final Runnable threadChecker;
    private final MemoryLedger ledger;
    private final StableMemoryBackend stableMemoryBackend;
    private final YierdisDbHealth health;

    public YierdisDbMutationExecutor(
            Runnable threadChecker,
            MemoryLedger ledger,
            StableMemoryBackend stableMemoryBackend,
            YierdisDbHealth health
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.stableMemoryBackend = stableMemoryBackend;
        this.health = Objects.requireNonNull(health, "health");
    }

    public <T> T execute(MutationPlan<T> plan) {
        Objects.requireNonNull(plan, "plan");
        threadChecker.run();
        health.requireWritable();
        if (stableMemoryBackend == null) {
            throw new IllegalStateException("prepared mutations require a native allocator");
        }
        return executePrepared(plan);
    }

    private <T> T executePrepared(MutationPlan<T> plan) {
        MemoryReservation reservation = null;
        NativeAllocationScope allocations = null;
        PreparedDbMutation<T> prepared = null;
        boolean commitStarted = false;
        boolean allocationsPromoted = false;
        boolean ledgerSettled = false;
        boolean nativePageTrimAttempted = false;
        try {
            boolean reclamation = plan.admissionMode() == MutationPlan.AdmissionMode.RECLAMATION;
            reservation = reclamation ? ledger.beginReclamation() : reserveNormalPlan(plan);
            allocations = stableMemoryBackend.beginAllocationScope();
            prepared = Objects.requireNonNull(plan.prepare(), "prepared mutation");
            NativeAllocationGrowth nativeGrowth = allocations.growth();
            long preparedPeakBytes = MemoryUsageSnapshot.addSaturating(
                    nativeGrowth.effectiveBytes(),
                    prepared.stagedNonNativeGrowthBytes()
            );
            if (reclamation) {
                requireReclamationInvariants(
                        plan.upperBoundBytes(),
                        nativeGrowth,
                        prepared.stagedNonNativeGrowthBytes(),
                        prepared.actualDeltaBytes()
                );
            } else {
                ledger.reconcile(reservation, preparedPeakBytes);
            }
            requireLedgerDeltaInvariant(ledger.usedBytes(), prepared.actualDeltaBytes());

            commitStarted = true;
            T result = prepared.commit();
            allocations.promote();
            allocationsPromoted = true;
            ledger.commit(reservation, prepared.actualDeltaBytes());
            ledgerSettled = true;
            prepared.releaseSuperseded();
            if (ledger.maxmemoryEnabled() && prepared.shouldTrimNativePagesAfterCommit()) {
                nativePageTrimAttempted = true;
                stableMemoryBackend.trimEmptyPages(MemoryPressureBudget.UNLIMITED);
            }
            return result;
        } catch (MemoryLedgerOutOfMemoryException | NativeCapacityExceededException expected) {
            if (!commitStarted) {
                abortBeforeCommit(prepared, allocations, reservation, expected);
                throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
            }
            IllegalStateException invariantFailure = new IllegalStateException(
                    "capacity failure after commit started",
                    expected
            );
            settleAfterCommit(
                    prepared,
                    allocations,
                    reservation,
                    allocationsPromoted,
                    ledgerSettled,
                    nativePageTrimAttempted,
                    invariantFailure
            );
            throw postCommitFailure(invariantFailure);
        } catch (RuntimeException | Error failure) {
            if (!commitStarted) {
                abortBeforeCommit(prepared, allocations, reservation, failure);
                if (isDegradingInvariantFailure(failure)) {
                    health.recordInvariantFailure(failure);
                }
            } else {
                settleAfterCommit(
                        prepared,
                        allocations,
                        reservation,
                        allocationsPromoted,
                        ledgerSettled,
                        nativePageTrimAttempted,
                        failure
                );
                throw postCommitFailure(failure);
            }
            throw failure;
        }
    }

    private MemoryReservation reserveNormalPlan(MutationPlan<?> plan) {
        long admittedUpperBound = normalUpperBound(plan);
        while (true) {
            final MemoryReservation reservation;
            try {
                reservation = ledger.reserve(admittedUpperBound);
            } catch (MemoryLedgerOutOfMemoryException rejected) {
                long refinedUpperBound = normalUpperBound(plan);
                if (refinedUpperBound >= admittedUpperBound) {
                    throw rejected;
                }
                admittedUpperBound = refinedUpperBound;
                continue;
            }
            final long refinedUpperBound;
            try {
                refinedUpperBound = normalUpperBound(plan);
            } catch (RuntimeException | Error failure) {
                rollbackReadmissionReservation(reservation, failure);
                throw failure;
            }
            if (refinedUpperBound <= admittedUpperBound) {
                return reservation;
            }
            ledger.rollback(reservation);
            admittedUpperBound = refinedUpperBound;
        }
    }

    private static long normalUpperBound(MutationPlan<?> plan) {
        return Math.max(0L, plan.upperBoundBytes());
    }

    private void rollbackReadmissionReservation(MemoryReservation reservation, Throwable failure) {
        try {
            ledger.rollback(reservation);
        } catch (RuntimeException | Error rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private void abortBeforeCommit(
            PreparedDbMutation<?> prepared,
            NativeAllocationScope allocations,
            MemoryReservation reservation,
            Throwable failure
    ) {
        try {
            if (prepared != null) {
                prepared.abort();
            }
        } catch (RuntimeException | Error abortFailure) {
            failure.addSuppressed(abortFailure);
        }
        try {
            if (allocations != null) {
                allocations.abort();
            }
        } catch (RuntimeException | Error abortFailure) {
            failure.addSuppressed(abortFailure);
        }
        try {
            ledger.rollback(reservation);
        } catch (RuntimeException | Error rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private void settleAfterCommit(
            PreparedDbMutation<?> prepared,
            NativeAllocationScope allocations,
            MemoryReservation reservation,
            boolean allocationsPromoted,
            boolean ledgerSettled,
            boolean nativePageTrimAttempted,
            Throwable failure
    ) {
        if (!allocationsPromoted && allocations != null) {
            try {
                allocations.promote();
            } catch (RuntimeException | Error promotionFailure) {
                failure.addSuppressed(promotionFailure);
            }
        }
        if (!ledgerSettled && prepared != null) {
            try {
                ledger.commit(reservation, prepared.actualDeltaBytes());
            } catch (RuntimeException | Error settlementFailure) {
                failure.addSuppressed(settlementFailure);
            }
        }
        if (prepared != null) {
            try {
                prepared.releaseSuperseded();
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            if (!nativePageTrimAttempted
                    && ledger.maxmemoryEnabled()
                    && prepared.shouldTrimNativePagesAfterCommit()) {
                try {
                    stableMemoryBackend.trimEmptyPages(MemoryPressureBudget.UNLIMITED);
                } catch (RuntimeException | Error trimFailure) {
                    failure.addSuppressed(trimFailure);
                }
            }
        }
    }

    private PostCommitMutationException postCommitFailure(Throwable failure) {
        health.recordInvariantFailure(failure);
        return new PostCommitMutationException("mutation failed after commit started", failure);
    }

    private static boolean isDegradingInvariantFailure(Throwable failure) {
        return failure instanceof NativeMemoryException || failure instanceof IllegalStateException;
    }

    private static void requireReclamationInvariants(
            long upperBoundBytes,
            NativeAllocationGrowth nativeGrowth,
            long stagedNonNativeGrowthBytes,
            long actualDeltaBytes
    ) {
        if (upperBoundBytes != 0L) {
            throw new IllegalStateException("reclamation mutation must have a zero upper bound");
        }
        if (nativeGrowth.nativeMetadataCommittedBytes() != 0L
                || nativeGrowth.nativeDataCommittedBytes() != 0L
                || stagedNonNativeGrowthBytes != 0L) {
            throw new IllegalStateException("reclamation mutation must not stage positive growth");
        }
        if (actualDeltaBytes > 0L) {
            throw new IllegalStateException("reclamation mutation must not commit positive growth");
        }
    }

    private static void requireLedgerDeltaInvariant(long usedBytes, long actualDeltaBytes) {
        final long nextUsedBytes;
        try {
            nextUsedBytes = Math.addExact(usedBytes, actualDeltaBytes);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("prepared mutation overflows ledger usage", overflow);
        }
        if (nextUsedBytes < 0L) {
            throw new IllegalStateException("prepared mutation underflows ledger usage");
        }
    }

    public interface MutationPlan<T> {
        enum AdmissionMode {
            NORMAL,
            RECLAMATION
        }

        long upperBoundBytes();

        default AdmissionMode admissionMode() {
            return AdmissionMode.NORMAL;
        }

        PreparedDbMutation<T> prepare();
    }

}
