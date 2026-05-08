package yier.bubu.redis.storage.memory.internal.ledger;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryReservation;
import yier.bubu.redis.memory.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.util.Objects;

public final class YierdisDbMutationExecutor {
    private final Runnable threadChecker;
    private final MemoryLedger ledger;

    public YierdisDbMutationExecutor(YierdisDb db) {
        this(
                Objects.requireNonNull(db, "db")::checkThread,
                db.memoryLedger()
        );
    }

    public YierdisDbMutationExecutor(Runnable threadChecker, MemoryLedger ledger) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    public <T> T execute(MutationPlan<T> plan) {
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

    public interface MutationPlan<T> {
        long upperBoundBytes();

        MutationResult<T> apply();
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
