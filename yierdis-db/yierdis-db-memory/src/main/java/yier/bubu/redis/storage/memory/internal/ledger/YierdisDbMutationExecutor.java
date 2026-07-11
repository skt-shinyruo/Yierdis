package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.YierdisDb;

public final class YierdisDbMutationExecutor {
    private final Runnable threadChecker;
    private final MemoryLedger ledger;
    private final NativeAllocator nativeAllocator;

    public YierdisDbMutationExecutor(YierdisDb db) {
        this(
                Objects.requireNonNull(db, "db")::checkThread,
                db.memoryLedger(),
                db.nativeAllocator()
        );
    }

    public YierdisDbMutationExecutor(Runnable threadChecker, MemoryLedger ledger) {
        this(threadChecker, ledger, null);
    }

    public YierdisDbMutationExecutor(
            Runnable threadChecker,
            MemoryLedger ledger,
            NativeAllocator nativeAllocator
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.nativeAllocator = nativeAllocator;
    }

    public <T> T execute(MutationPlan<T> plan) {
        Objects.requireNonNull(plan, "plan");
        threadChecker.run();
        if (plan instanceof LegacyMutationPlan<?> legacy) {
            return executeLegacy(castLegacy(legacy));
        }
        if (nativeAllocator == null) {
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
        try {
            reservation = plan.admissionMode() == MutationPlan.AdmissionMode.RECLAMATION
                    ? ledger.beginReclamation()
                    : ledger.reserve(Math.max(0L, plan.upperBoundBytes()));
            allocations = nativeAllocator.beginAllocationScope();
            prepared = Objects.requireNonNull(plan.prepare(), "prepared mutation");
            long preparedPeakBytes = MemoryUsageSnapshot.addSaturating(
                    allocations.growth().effectiveBytes(),
                    prepared.stagedNonNativeGrowthBytes()
            );
            if (plan.admissionMode() == MutationPlan.AdmissionMode.RECLAMATION) {
                requireReclamationInvariants(
                        plan.upperBoundBytes(),
                        preparedPeakBytes,
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
                    invariantFailure
            );
            throw invariantFailure;
        } catch (RuntimeException | Error failure) {
            if (!commitStarted) {
                abortBeforeCommit(prepared, allocations, reservation, failure);
            } else {
                settleAfterCommit(
                        prepared,
                        allocations,
                        reservation,
                        allocationsPromoted,
                        ledgerSettled,
                        failure
                );
            }
            throw failure;
        }
    }

    private <T> T executeLegacy(LegacyMutationPlan<T> plan) {
        MemoryReservation reservation = null;
        try {
            reservation = ledger.reserve(Math.max(0L, plan.upperBoundBytes()));
            MutationResult<T> result = Objects.requireNonNull(plan.apply(), "mutation result");
            ledger.commit(reservation, result.actualDeltaBytes());
            return result.value();
        } catch (MemoryLedgerOutOfMemoryException | OffHeapOutOfMemoryException expected) {
            ledger.rollback(reservation);
            throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
        } catch (RuntimeException | Error failure) {
            ledger.rollback(reservation);
            throw failure;
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
        } catch (RuntimeException abortFailure) {
            failure.addSuppressed(abortFailure);
        }
        try {
            if (allocations != null) {
                allocations.abort();
            }
        } catch (RuntimeException abortFailure) {
            failure.addSuppressed(abortFailure);
        }
        try {
            ledger.rollback(reservation);
        } catch (RuntimeException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private void settleAfterCommit(
            PreparedDbMutation<?> prepared,
            NativeAllocationScope allocations,
            MemoryReservation reservation,
            boolean allocationsPromoted,
            boolean ledgerSettled,
            Throwable failure
    ) {
        if (!allocationsPromoted && allocations != null) {
            try {
                allocations.promote();
            } catch (RuntimeException promotionFailure) {
                failure.addSuppressed(promotionFailure);
            }
        }
        if (!ledgerSettled && prepared != null) {
            try {
                ledger.commit(reservation, prepared.actualDeltaBytes());
            } catch (RuntimeException settlementFailure) {
                failure.addSuppressed(settlementFailure);
            }
        }
    }

    private static void requireReclamationInvariants(
            long upperBoundBytes,
            long preparedPeakBytes,
            long actualDeltaBytes
    ) {
        if (upperBoundBytes != 0L) {
            throw new IllegalStateException("reclamation mutation must have a zero upper bound");
        }
        if (preparedPeakBytes != 0L) {
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

    @SuppressWarnings("unchecked")
    private static <T> LegacyMutationPlan<T> castLegacy(LegacyMutationPlan<?> plan) {
        return (LegacyMutationPlan<T>) plan;
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

    public interface LegacyMutationPlan<T> extends MutationPlan<T> {
        MutationResult<T> apply();

        @Override
        default PreparedDbMutation<T> prepare() {
            MutationResult<T> result = Objects.requireNonNull(apply(), "mutation result");
            return new AbstractPreparedMutation<T>(
                    result.actualDeltaBytes(),
                    0L,
                    MutationOutcome.NONE
            ) {
                @Override
                protected T commitPrepared() {
                    return result.value();
                }

                @Override
                protected void releaseSupersededPrepared() {
                }

                @Override
                protected void abortPrepared() {
                }
            };
        }
    }

    public static final class MutationResult<T> {
        private final T value;
        private final long actualDeltaBytes;

        private MutationResult(T value, long actualDeltaBytes) {
            this.value = value;
            this.actualDeltaBytes = actualDeltaBytes;
        }

        public static <T> MutationResult<T> of(T value, long actualDeltaBytes) {
            return new MutationResult<>(value, actualDeltaBytes);
        }

        public T value() {
            return value;
        }

        public long actualDeltaBytes() {
            return actualDeltaBytes;
        }
    }
}
