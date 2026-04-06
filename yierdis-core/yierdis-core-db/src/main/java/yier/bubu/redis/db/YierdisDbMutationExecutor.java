package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.db.memory.MemoryLedger;
import yier.bubu.redis.db.memory.MemoryReservation;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.ops.YierdisCommandException;

import java.util.Objects;

final class YierdisDbMutationExecutor {
    private final Runnable threadChecker;
    private final MemoryLedger ledger;

    YierdisDbMutationExecutor(YierdisDb db) {
        this(
                Objects.requireNonNull(db, "db")::checkThread,
                db.memoryLedger()
        );
    }

    YierdisDbMutationExecutor(Runnable threadChecker, MemoryLedger ledger) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    <T> T execute(MutationPlan<T> plan) {
        Objects.requireNonNull(plan, "plan");
        threadChecker.run();

        MemoryReservation reservation = null;
        try {
            reservation = ledger.reserve(Math.max(0L, plan.upperBoundBytes()));
            MutationResult<T> result = Objects.requireNonNull(plan.apply(), "mutation result");
            ledger.commit(reservation, result.actualDeltaBytes());
            return result.value();
        } catch (MemoryLedgerOutOfMemoryException e) {
            ledger.rollback(reservation);
            throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
        } catch (OffHeapOutOfMemoryException e) {
            ledger.rollback(reservation);
            throw new YierdisCommandException("OOM off-heap memory limit exceeded");
        } catch (RuntimeException | Error e) {
            ledger.rollback(reservation);
            throw e;
        }
    }

    interface MutationPlan<T> {
        long upperBoundBytes();

        MutationResult<T> apply();
    }

    static final class MutationResult<T> {
        private final T value;
        private final long actualDeltaBytes;

        private MutationResult(T value, long actualDeltaBytes) {
            this.value = value;
            this.actualDeltaBytes = actualDeltaBytes;
        }

        static <T> MutationResult<T> of(T value, long actualDeltaBytes) {
            return new MutationResult<>(value, actualDeltaBytes);
        }

        T value() {
            return value;
        }

        long actualDeltaBytes() {
            return actualDeltaBytes;
        }
    }
}
