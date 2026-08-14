package yier.bubu.redis.storage.memory.internal.ledger;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

// InMemoryLedger：最小可测试实现，用于锁定 MemoryLedger 的 reserve/commit/rollback 不变量。

import java.util.Objects;

/**
 * A minimal in-memory {@link MemoryLedger} implementation intended for contract tests and initial landing.
 * <p>
 * This implementation is intentionally strict:
 * - no negative reservation
 * - no reservation double-finish
 * - prevents usedBytes underflow
 */
public final class InMemoryLedger implements MemoryLedger {
    private final long limitBytes;
    private long usedBytes;
    private long reservedBytes;
    private int normalReservations;
    private int reclamationBegins;

    public InMemoryLedger(long limitBytes) {
        if (limitBytes < 0) {
            throw new IllegalArgumentException("limitBytes must be >= 0");
        }
        this.limitBytes = limitBytes;
    }

    @Override
    public long limitBytes() {
        return limitBytes;
    }

    @Override
    public long usedBytes() {
        return usedBytes;
    }

    @Override
    public long reservedBytes() {
        return reservedBytes;
    }

    @Override
    public MemoryReservation reserve(long estimatedExtraBytes) {
        normalReservations++;
        if (estimatedExtraBytes < 0) {
            throw new IllegalArgumentException("estimatedExtraBytes must be >= 0");
        }
        if (estimatedExtraBytes == 0) {
            return NoopReservation.INSTANCE;
        }

        long nextReservedBytes = checkedReservationTotal(reservedBytes, estimatedExtraBytes);
        if (limitBytes > 0) {
            long next = checkedEffectiveTotal(usedBytes, nextReservedBytes);
            if (next > limitBytes) {
                throw new MemoryLedgerOutOfMemoryException();
            }
        }

        reservedBytes = nextReservedBytes;
        return new ReservationToken(this, estimatedExtraBytes);
    }

    @Override
    public void reconcile(MemoryReservation reservation, long requiredBytes) {
        if (requiredBytes < 0) {
            throw new IllegalArgumentException("requiredBytes must be >= 0");
        }
        ReservationToken token = ReservationToken.validate(reservation, this);
        long reserved = token == null ? 0L : token.reservedBytes;
        if (requiredBytes > reserved) {
            throw new IllegalStateException("prepared mutation exceeded its reservation");
        }
        if (token != null && requiredBytes < reserved) {
            reservedBytes -= reserved - requiredBytes;
            token.reservedBytes = requiredBytes;
        }
    }

    @Override
    public MemoryReservation beginReclamation() {
        reclamationBegins++;
        return new ReservationToken(this, 0L);
    }

    @Override
    public void commit(MemoryReservation reservation, long actualDeltaBytes) {
        ReservationToken token = ReservationToken.validate(reservation, this);
        long nextUsedBytes = checkedCommittedUsage(usedBytes, actualDeltaBytes);
        if (token != null) {
            token.finish();
            reservedBytes -= token.reservedBytes;
            if (reservedBytes < 0) {
                throw new IllegalStateException("reservedBytes underflow");
            }
        }

        usedBytes = nextUsedBytes;
    }

    @Override
    public void rollback(MemoryReservation reservation) {
        ReservationToken token = ReservationToken.validate(reservation, this);
        if (token == null) {
            return;
        }
        token.finish();
        reservedBytes -= token.reservedBytes;
        if (reservedBytes < 0) {
            throw new IllegalStateException("reservedBytes underflow");
        }
    }

    int normalReservations() {
        return normalReservations;
    }

    int reclamationBegins() {
        return reclamationBegins;
    }

    private static long checkedReservationTotal(long current, long increment) {
        try {
            return Math.addExact(current, increment);
        } catch (ArithmeticException overflow) {
            throw new MemoryLedgerOutOfMemoryException();
        }
    }

    private static long checkedEffectiveTotal(long usedBytes, long reservedBytes) {
        try {
            return Math.addExact(usedBytes, reservedBytes);
        } catch (ArithmeticException overflow) {
            throw new MemoryLedgerOutOfMemoryException();
        }
    }

    private static long checkedCommittedUsage(long current, long delta) {
        final long next;
        try {
            next = Math.addExact(current, delta);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("usedBytes overflow", overflow);
        }
        if (next < 0L) {
            throw new IllegalStateException("usedBytes underflow");
        }
        return next;
    }

    private enum NoopReservation implements MemoryReservation {
        INSTANCE;

        @Override
        public long reservedBytes() {
            return 0;
        }
    }

    private static final class ReservationToken implements MemoryReservation {
        private final InMemoryLedger owner;
        private long reservedBytes;
        private boolean finished;

        private ReservationToken(InMemoryLedger owner, long reservedBytes) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.reservedBytes = reservedBytes;
        }

        @Override
        public long reservedBytes() {
            return reservedBytes;
        }

        private void finish() {
            if (finished) {
                throw new IllegalStateException("reservation already finished");
            }
            finished = true;
        }

        private static ReservationToken validate(MemoryReservation reservation, InMemoryLedger expectedOwner) {
            if (reservation == null) {
                return null;
            }
            if (reservation instanceof NoopReservation) {
                return null;
            }
            if (!(reservation instanceof ReservationToken token)) {
                throw new IllegalArgumentException("unknown reservation type: " + reservation.getClass().getName());
            }
            if (token.owner != expectedOwner) {
                throw new IllegalArgumentException("reservation does not belong to this ledger");
            }
            return token;
        }
    }
}
