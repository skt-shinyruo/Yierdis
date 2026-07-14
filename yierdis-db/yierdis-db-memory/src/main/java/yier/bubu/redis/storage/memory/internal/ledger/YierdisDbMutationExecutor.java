package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import yier.bubu.redis.common.command.CommandRecordScope;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.DbCommitReservation;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.memory.YierdisDbHealth;

public final class YierdisDbMutationExecutor {
    private final Runnable threadChecker;
    private final MemoryLedger ledger;
    private final NativeAllocator nativeAllocator;
    private final YierdisDbHealth health;
    private final Supplier<DbCommitPublisher> commitPublisherSupplier;
    private final IntSupplier commitDbIndexSupplier;
    private final LongSupplier clockMillis;

    public YierdisDbMutationExecutor(YierdisDb db) {
        this(
                Objects.requireNonNull(db, "db")::checkThread,
                db.memoryLedger(),
                db.nativeAllocator(),
                db.healthMonitor(),
                db::commitPublisher,
                db::commitDbIndex
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
        this(
                threadChecker,
                ledger,
                nativeAllocator,
                new YierdisDbHealth(Objects.requireNonNull(threadChecker, "threadChecker"))
        );
    }

    public YierdisDbMutationExecutor(
            Runnable threadChecker,
            MemoryLedger ledger,
            NativeAllocator nativeAllocator,
            YierdisDbHealth health
    ) {
        this(
                threadChecker,
                ledger,
                nativeAllocator,
                health,
                () -> DbCommitPublisher.NOOP,
                () -> 0
        );
    }

    public YierdisDbMutationExecutor(
            Runnable threadChecker,
            MemoryLedger ledger,
            NativeAllocator nativeAllocator,
            YierdisDbHealth health,
            Supplier<DbCommitPublisher> commitPublisherSupplier,
            IntSupplier commitDbIndexSupplier
    ) {
        this(
                threadChecker,
                ledger,
                nativeAllocator,
                health,
                commitPublisherSupplier,
                commitDbIndexSupplier,
                System::currentTimeMillis
        );
    }

    YierdisDbMutationExecutor(
            Runnable threadChecker,
            MemoryLedger ledger,
            NativeAllocator nativeAllocator,
            YierdisDbHealth health,
            Supplier<DbCommitPublisher> commitPublisherSupplier,
            IntSupplier commitDbIndexSupplier,
            LongSupplier clockMillis
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.nativeAllocator = nativeAllocator;
        this.health = Objects.requireNonNull(health, "health");
        this.commitPublisherSupplier = Objects.requireNonNull(commitPublisherSupplier, "commitPublisherSupplier");
        this.commitDbIndexSupplier = Objects.requireNonNull(commitDbIndexSupplier, "commitDbIndexSupplier");
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
    }

    public <T> T execute(MutationPlan<T> plan) {
        Objects.requireNonNull(plan, "plan");
        threadChecker.run();
        health.requireWritable();
        DbCommitPublisher publisher = commitPublisher();
        requireCommitStreamAvailability(plan, publisher);
        if (plan instanceof LegacyMutationPlan<?> legacy) {
            if (plan.requiresCommitStream() && publisher.enabled()) {
                throw new IllegalStateException("legacy mutations cannot bypass the commit stream");
            }
            return executeLegacy(castLegacy(legacy));
        }
        if (nativeAllocator == null) {
            throw new IllegalStateException("prepared mutations require a native allocator");
        }
        return executePrepared(plan, publisher);
    }

    private <T> T executePrepared(MutationPlan<T> plan, DbCommitPublisher publisher) {
        MemoryReservation reservation = null;
        NativeAllocationScope allocations = null;
        PreparedDbMutation<T> prepared = null;
        DbCommitReservation commitReservation = null;
        ImmutableCommandRecord commitRecord = null;
        boolean commitStarted = false;
        boolean allocationsPromoted = false;
        boolean ledgerSettled = false;
        boolean publishChanges = plan.requiresCommitStream() && publisher.enabled();
        try {
            boolean reclamation = plan.admissionMode() == MutationPlan.AdmissionMode.RECLAMATION;
            reservation = reclamation ? ledger.beginReclamation() : reserveNormalPlan(plan);
            allocations = nativeAllocator.beginAllocationScope();
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

            if (publishChanges && prepared.outcome().changedAny()) {
                commitRecord = plan.retainCommitRecord();
                commitReservation = publisher.reserve(
                        commitDbIndexSupplier.getAsInt(),
                        plan.commitKind(),
                        commitRecord,
                        prepared.actualDeltaBytes(),
                        clockMillis.getAsLong()
                );
                commitRecord.close();
                commitRecord = null;
            }

            commitStarted = true;
            T result = prepared.commit();
            allocations.promote();
            allocationsPromoted = true;
            ledger.commit(reservation, prepared.actualDeltaBytes());
            ledgerSettled = true;
            if (commitReservation != null) {
                publisher.publish(commitReservation);
                commitReservation = null;
            }
            prepared.releaseSuperseded();
            if (ledger.maxmemoryEnabled() && prepared.actualDeltaBytes() <= 0L) {
                nativeAllocator.trimEmptyPages(MemoryPressureBudget.unlimited());
            }
            return result;
        } catch (MemoryLedgerOutOfMemoryException | NativeCapacityExceededException expected) {
            if (!commitStarted) {
                abortBeforeCommit(prepared, allocations, commitReservation, reservation, commitRecord, expected);
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
            failPublicationAfterCommit(publisher, commitReservation, invariantFailure);
            throw postCommitFailure(invariantFailure);
        } catch (RuntimeException | Error failure) {
            if (!commitStarted) {
                abortBeforeCommit(prepared, allocations, commitReservation, reservation, commitRecord, failure);
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
                        failure
                );
                failPublicationAfterCommit(publisher, commitReservation, failure);
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

    private <T> T executeLegacy(LegacyMutationPlan<T> plan) {
        MemoryReservation reservation = null;
        boolean mutationStarted = false;
        boolean ledgerSettled = false;
        try {
            boolean reclamation = plan.admissionMode() == MutationPlan.AdmissionMode.RECLAMATION;
            if (reclamation && plan.upperBoundBytes() != 0L) {
                throw new IllegalStateException("reclamation mutation must have a zero upper bound");
            }
            reservation = reclamation
                    ? ledger.beginReclamation()
                    : ledger.reserve(Math.max(0L, plan.upperBoundBytes()));
            plan.validateBeforeMutation();
            mutationStarted = true;
            MutationResult<T> result = Objects.requireNonNull(plan.apply(), "mutation result");
            if (reclamation && result.actualDeltaBytes() > 0L) {
                throw new IllegalStateException("reclamation mutation must not commit positive growth");
            }
            requireLedgerDeltaInvariant(ledger.usedBytes(), result.actualDeltaBytes());
            ledger.commit(reservation, result.actualDeltaBytes());
            ledgerSettled = true;
            return result.value();
        } catch (MemoryLedgerOutOfMemoryException | NativeCapacityExceededException expected) {
            if (!mutationStarted) {
                ledger.rollback(reservation);
                throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
            }
            IllegalStateException invariantFailure = new IllegalStateException(
                    "capacity failure after legacy mutation started",
                    expected
            );
            settleLegacyAfterMutationStarted(plan, reservation, ledgerSettled, invariantFailure);
            throw postCommitFailure(invariantFailure);
        } catch (RuntimeException | Error failure) {
            if (!mutationStarted) {
                ledger.rollback(reservation);
                if (isDegradingInvariantFailure(failure)) {
                    health.recordInvariantFailure(failure);
                }
                throw failure;
            }
            settleLegacyAfterMutationStarted(plan, reservation, ledgerSettled, failure);
            throw postCommitFailure(failure);
        }
    }

    private void settleLegacyAfterMutationStarted(
            LegacyMutationPlan<?> plan,
            MemoryReservation reservation,
            boolean ledgerSettled,
            Throwable failure
    ) {
        if (ledgerSettled) {
            return;
        }
        long conservativeDeltaBytes = plan.admissionMode() == MutationPlan.AdmissionMode.RECLAMATION
                ? 0L
                : reservation == null ? 0L : reservation.reservedBytes();
        try {
            requireLedgerDeltaInvariant(ledger.usedBytes(), conservativeDeltaBytes);
            ledger.commit(reservation, conservativeDeltaBytes);
        } catch (RuntimeException | Error settlementFailure) {
            failure.addSuppressed(settlementFailure);
        }
    }

    private void abortBeforeCommit(
            PreparedDbMutation<?> prepared,
            NativeAllocationScope allocations,
            DbCommitReservation commitReservation,
            MemoryReservation reservation,
            ImmutableCommandRecord commitRecord,
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
            if (commitReservation != null) {
                commitReservation.close();
            }
        } catch (RuntimeException | Error abortFailure) {
            failure.addSuppressed(abortFailure);
        }
        try {
            ledger.rollback(reservation);
        } catch (RuntimeException | Error rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
        try {
            if (commitRecord != null) {
                commitRecord.close();
            }
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
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
    }

    private PostCommitMutationException postCommitFailure(Throwable failure) {
        health.recordInvariantFailure(failure);
        return new PostCommitMutationException("mutation failed after commit started", failure);
    }

    private DbCommitPublisher commitPublisher() {
        return Objects.requireNonNull(commitPublisherSupplier.get(), "commit publisher");
    }

    private static void requireCommitStreamAvailability(MutationPlan<?> plan, DbCommitPublisher publisher) {
        if (plan.requiresCommitStream() && publisher.enabled() && !publisher.available()) {
            throw new DbCommitStreamUnavailableException();
        }
    }

    private static void failPublicationAfterCommit(
            DbCommitPublisher publisher,
            DbCommitReservation reservation,
            Throwable failure
    ) {
        if (reservation == null) {
            return;
        }
        try {
            publisher.failAfterCommit(reservation);
        } catch (RuntimeException | Error publicationFailure) {
            failure.addSuppressed(publicationFailure);
        }
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

        default DbCommitKind commitKind() {
            return DbCommitKind.USER;
        }

        default boolean requiresCommitStream() {
            return true;
        }

        default ImmutableCommandRecord retainCommitRecord() {
            ImmutableCommandRecord current = CommandRecordScope.current();
            if (current == null) {
                throw new DbCommitStreamUnavailableException();
            }
            return current.retain();
        }

        PreparedDbMutation<T> prepare();
    }

    public interface LegacyMutationPlan<T> extends MutationPlan<T> {
        /**
         * 在 legacy mutation 触及可见状态前执行的无副作用校验。
         */
        default void validateBeforeMutation() {
        }

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
