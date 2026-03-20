package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.db.memory.MemoryReservation;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.ops.YierdisCommandException;

import java.util.Objects;

final class YierdisDbMutationExecutor {
    private final YierdisDb db;

    YierdisDbMutationExecutor(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    <T> T execute(MutationPlan<T> plan) {
        Objects.requireNonNull(plan, "plan");
        db.checkThread();

        MemoryReservation reservation = null;
        try {
            reservation = db.reserveMutation(plan.upperBoundBytes());
            MutationResult<T> result = Objects.requireNonNull(plan.apply(), "mutation result");
            db.commitMutation(reservation, result.actualDeltaBytes());
            return result.value();
        } catch (MemoryLedgerOutOfMemoryException e) {
            db.rollbackMutation(reservation);
            throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
        } catch (OffHeapOutOfMemoryException e) {
            db.rollbackMutation(reservation);
            throw new YierdisCommandException("OOM off-heap memory limit exceeded");
        } catch (RuntimeException | Error e) {
            db.rollbackMutation(reservation);
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
